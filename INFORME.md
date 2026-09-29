# Informe TP Coordinación - Pedro Ciliberto

A continuación escibiré el informe del trabajo práctico de coordinación. En primer lugar, iré describiendo los cambios que realicé en el código de forma incremental, intentando cubrir paso por paso los **escenarios** descritos en la consigna. Luego, explicaré los **problemas de sincronización** que surgieron durante la implementación y cómo los resolví.

## Seguimiento de escenarios

### Escenario 1: manejo de un único cliente

Con este escenario se recibió el trabajo práctico. Lo único que podía hacer el sistema era procesar un único cliente, y lo único que hice fue **agregar mi implementación del Middleware** (traído de la entrega anterior).

### Escenario 2: manejo de múltiples clientes

Para poder manejar múltiples clientes, decidí que cada cliente debería tener un identificador único. Para esto, modifiqué la clase `MessageHandler` para que genere un `client_id` único al instanciarse. Esto se logra utilizando la librería `uuid` de Python.

Al cubrir este caso, surgieron cambios en otros sectores del código:

- Se modificaron los métodos de serialización para anteceder el `client_id` al principio de los mensajes, tanto en los mensajes de datos (`[client_id, fruit, amount]`) como en los mensajes de control de fin de transmisión (`[client_id]`).
- Hubo que modificar la estructura de acumulación de frutas en `SumFilter`. Se reemplazó por un diccionario indexado por cliente (`{ client_id: { fruit: FruitItem } }`).
- En `AggregationFilter`, se adaptó el ranking de tops parciales para mantener una lista ordenada **independiente por cliente**.

Ahora al recibir un mensaje de resultado, se verifica que el `client_id` del mensaje coincida con el `client_id` del `MessageHandler` que lo está procesando. Si no coinciden, se envía `None`, haciendo que el `gateaway` ignore el mensaje y pruebe con el siguiente. Por su parte, los tops parciales se mantienen separados por cliente, y al recibir un mensaje de `EOF`, se envía el **top parcial correspondiente a ese cliente**.

### Escenario 3: escalabilidad de la etapa de `Sum`

Para resolver el cuello de botella de procesamiento en la etapa `Sum` (en donde no se podía escalar el sistema respecto a grandes volúmenes de datos), se agregó la capacidad de **manejar $N$ instancias concurrentes** (`sum_0`, `sum_1`, etc.) que compiten por consumir los mensajes de una misma cola de entrada (`INPUT_QUEUE`).

Esto trajo desafíos de sincronización que requirieron los siguientes cambios:

- Dado que los datos de un mismo cliente ahora se reparten entre los distintos nodos de `Sum` y la cola de trabajo entrega cada mensaje a un único consumidor, el mensaje de **EOF** enviado por el Gateway es recibido por **una sola instancia** de `Sum`. Para notificar al resto de las instancias, la receptora de ese mensaje retransmite el `EOF` a un canal de control global entre las instancias de `Sum` (`SUM_CONTROL_EXCHANGE`). Cada una mantiene un **hilo secundario** escuchando en este canal para capturar el momento en el que el Gateway termina de enviar los datos, enviar sus resultados parciales acumulados en `Aggregation` y enviar su propia confirmación individual de finalización.
- En `AggregationFilter`, se modificó la lógica para evitar que se emita el ranking final de forma prematura al recibir la primera notificación de fin. Se incorporó un **contador de confirmaciones por cliente** (`client_eof_counts`) que guarda cuántos mensajes `EOF` se han recibido por cada uno. Una vez se llega a la cantidad esperada `SUM_AMOUNT`, se procede a generar el top final de ese Aggregator y enviarlo al controlador **Join**.
- Para garantizar la consistencia de los datos que llegan de las distintas instancias de `Sum`, en `AggregationFilter` se reemplazó el uso de `bisect.insort` por un diccionario interno (`{ client_id: { fruit: FruitItem } }`). De esta forma, las sumas parciales se acumulan de manera exacta a medida que llegan. El ordenamiento se realiza únicamente cuando el contador alcanza la cantidad esperada de nodos (`SUM_AMOUNT`).

### Escenario 4: Múltiples clientes y múltiples réplicas (varias instancias de `Sum` y `Aggregation`)

En esta parte se agregó escalbilidad la etapa `Aggregation` mediante $M$ réplicas concurrentes (`aggregation_0`, `aggregation_1`, `aggregation_2`, etc).

Con la introducción de múltiples instancias de `Aggregation`, se hicieron las siguientes modificaciones:

- Dentro de `Sum`, en lugar de realizar un *broadcast* de los datos que enviaría cada fruta a todas las instancias de `Aggregation` (lo que generaría procesamiento redundante y fragmentado), implementé un **particionado por clave (*sharding*)**. Para evitar los problemas de hashes aleatorios del `hash()` nativo entre procesos en Python, se utilizó la librería determinística `zlib` (que no es criptográfica). Esto garantiza que una fruta en particular sea derivada **siempre al mismo nodo `Aggregation`**, sin importar cuál instancia de `Sum` la haya procesado.
- A diferencia de los mensajes de datos, la señal de **EOF** de cada instancia `Sum` se sigue enviando mediante *broadcast* a **todas** las instancias de `Aggregation`. Cada nodo de `Aggregation` espera recibir exactamente `SUM_AMOUNT` notificaciones por cada cliente para concluir esa partición.
- Dado que cada `Aggregation` procesa un subconjunto disjunto de frutas, emite un **Top parcial** hacia el nodo `Join` (solo con las frutas que obtiene). Por eso, se adaptó `JoinFilter` de forma similar a lo hecho con las agregaciones en el escenario anterior: se mantiene un **contador por cliente** y acumula las respuestas parciales de las $M$ instancias (`AGGREGATION_AMOUNT`). Una vez recibidas los $M$ Tops, `JoinFilter` junta las tuplas, realiza un ordenamiento global y emite el Top definitivo hacia el `Gateway`.

### Escenario 5: Nombres al azar

A lo largo de la implementación, siempre utilicé los nombres recibidos por las variables de entorno para las colas y *exchanges*. Esto permitió que el sistema fuera **independiente de los nombres de colas y exchanges** y que pueda ser ejecutado utilizando cualquier nombre arbitrario sin necesidad de modificar el código. Por ende, este escenario no requirió cambios adicionales.

## Corrección de condición de carrera y sincronización en `Sum`

Inicialmente, durante la ejecución de los escenarios se detectó una **condición de carrera (*race condition*)** en la sincronización de las instancias de `Sum`. Al trabajar directamente en el envio de conteos parciales (y de forma inmediata), existía el riesgo de que el nodo coordinador declarase el fin de la transmisión (**EOF**) antes de que los demás nodos terminasen de procesar y vaciar (*flush*) sus mensajes de datos desde `Gateway`, o que estos quedasen bloqueados esperándolos. Consecuentemente, algunas frutas procesadas por los nodos de `Sum` **no llegaban a `Aggregation`**, generando inconsistencias en los resultados finales de *tops*.

Para solucionar este inconveniente y asegurar que **todas las frutas procesadas sean enviadas a la etapa de `Aggregation`**, se realizaron algunas modificaciones en la lógica de coordinación entre las instancias de `Sum`:

- Cuando el `Gateway` envía el **EOF** hacia una instancia de `Sum`, esta asume el rol de **Coordinador** para ese cliente. Lo que hace ahora es registrar el **total de mensajes** esperado que recibe en el payload del EOF enviado por el Gateway (`[client_id, total_messages]`) e inicia un *broadcast* hacia todas las demás instancias de `Sum`. Al recibir esta orden, cada nodo pasa al cliente a un "estado activo de reporte" (en el cual se encuentra en proceso de finalización) y envía todas las frutas acumuladas hasta ese momento hacia sus correspondientes nodos de `Aggregation` (aplicando el *sharding* por fruta).
- Una vez que cada nodo de `Sum` transfiere sus frutas acumuladas a `Aggregation`, envía un mensaje directo al Coordinador informando la **cantidad exacta de mensajes que procesó localmente** para dicho cliente. El Coordinador acumula estas respuestas y, únicamente **cuando la suma global alcanza el `total_messages` esperado**, emite una señal de finalización global (`EOF_ENDING = -1`).
- Si ingresan mensajes de datos de un cliente cuando ya se ha iniciado la fase de reporte, la instancia detecta que el identificador está en estado de reporte activo y retransmite inmediatamente la fruta a `Aggregation`, sumando dicha unidad al reporte enviado al Coordinador.
- Al recibir esa señal (`-1`), todas las instancias de `Sum` limpian las estructuras asociadas a ese `client_id` y transmiten el mensaje de `EOF` a cada uno de los nodos de `Aggregation`, garantizando que ningún dato quede retenido ni se emitan cierres prematuros.

# Informe TP Coordinación - Pedro Ciliberto

A continuación escibiré el informe del trabajo práctico de coordinación. En primer lugar, iré describiendo los cambios que realicé en el código de forma incremental, intentando cubrir paso por paso los escenarios descritos en la consigna.

## Seguimiento de escenarios

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

- Dado que los datos de un mismo cliente ahora se reparten entre los distintos nodos de `Sum` y la cola de trabajo entrega cada mensaje a un único consumidor, el mensaje de **EOF** enviado por el Gateway es recibido por **una sola instancia** de `Sum`. Para notificar al resto de las instancias, la receptora de ese mensaje retransmite el `EOF` a un canal de control global entre las instancias de `Sum` (`SUM_CONTROL_EXCHANGE`). Cada una mantiene un **hilo secundario** escuchando en este canal para capturar el momento en el que el Gateway termina de enviar los datos, enviar sus resultados parciales acumulados en `Aggregation` y enviar su propia confirmación individual de finalización (`[client_id, "SUM_EOF"]`).
- En `AggregationFilter`, se modificó la lógica para evitar que se emita el ranking final de forma prematura al recibir la primera notificación de fin. Se incorporó un **contador de confirmaciones por cliente** (`client_eof_counts`) que guarda cuántos mensajes `"SUM_EOF"` se han recibido por cada uno. Una vez se llega a la cantidad esperada `SUM_AMOUNT`, se procede a generar el top final de ese Aggregator y enviarlo al controlador **Join**.
- Para garantizar la consistencia de los datos que llegan de las distintas instancias de `Sum`, en `AggregationFilter` se reemplazó el uso de `bisect.insort` por un diccionario interno (`{ client_id: { fruit: FruitItem } }`). De esta forma, las sumas parciales se acumulan de manera exacta a medida que llegan. El ordenamiento se realiza únicamente cuando el contador alcanza la cantidad esperada de nodos (`SUM_AMOUNT`).

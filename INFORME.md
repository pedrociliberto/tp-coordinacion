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

# Coordinación y escalabilidad

 Gateway asigna un UUID por conexión/cliente, traduce los mensajes TCP a JSON sobre RabbitMQ y entrega el resultado al cliente correspondiente. Era necesario agregar un ID para identificar/separar la información y manejar distintos estados para distintos clientes.

## Entidades

### Sum

Las instancias de Sum reciben mensajes de una misma cola, en la cual Gateway publica mensajes de tipo `ClientMessage`. Se comunican entre sí mediante un exchange que permite difundir avisos a todos los Sum con la clave `all` o enviar mensajes a una instancia particular mediante una clave individual.

Cada Sum asume es el coordinador del cliente cuyo EOF recibe desde Gateway. Se encarga de avisar a los demás Sum que ese cliente terminó el envío, llevar la cuenta de sus mensajes procesados y comunicar el EOF a todos los Aggregation cuando todos los sums hayan hecho el flush.

Cada Sum clasifica a los clientes en uno de tres estados posibles. En `SENDING`, el estado inicial, acumula hasta 10 mensajes antes de flushear en los Aggregation correspondientes. Al recibir el EOF del Gateway o el aviso `EOF_NOTICE` de otro Sum, pasa a `UPDATING` y flushea. Desde entonces, flushea despues de cada mensaje pendiente inmediatamente y comunica su contador acumulado al coordinador. Si él mismo coordina ese cliente, verifica directamente si puede finalizar.

Cuando la suma de los contadores alcanza el total esperado, el coordinador envía el EOF a todos los Aggregation y broadcastea `COMPlETE` a los Sum. Cada Sum marca al cliente como `END` y elimina sus datos y contadores. Esta transición afecta únicamente a ese cliente, los demás continúan en sus propios estados.

### Aggregation

Cada Aggregation recibe mensajes de los Sum mediante una cola propia asociada a un exchange `direct`. El Sum calcula el destino con `FNV-1a("{UUID}-{fruta}") % AGGREGATION_AMOUNT`, va a haber un solo Aggregate trabajando cada client-fruta par.

En `SENDING`, acumula los registros por cliente y fruta usando `FruitItem.Sum`. Cuando recibe el EOF del Sum coordinador, pasa a `UPDATING`, envía todos sus registros a la cola del Join y después publica su propio EOF. Ambos mensajes son `ClientMessage` e incluyen su identificador en `coordinatorId`, para que el Join distinga qué partición aporta los datos y avisa su finalización. Un Aggregation sin datos para ese cliente envía una lista vacía y su EOF.

Despues del flush, pasa a `END`, libera la memoria y conserva la marca de finalización. Ignora EOF repetidos y considera un error recibir nuevos datos de ese cliente después de su EOF.

### Join

El Join recibe en una misma cola los mensajes de todos los Aggregation y guarda sus registros por cliente e identificador de emisor. Cada cliente comienza en `SENDING`; al recibir su primer EOF, pasa a `UPDATING` y continúa esperando los datos y EOF de las particiones pendientes.

Al recibir los EOFs de todos los Aggregates, combina los totales, los ordena mediante `FruitItem.Less` y selecciona hasta `TOP_SIZE` elementos. Publica el resultado con el UUID del cliente en la cola que consume Gateway. Finalmente pasa a `END`, libera la memoria y conserva la marca de finalización. Los datos recibidos después del EOF se ignoran.

## Mensajes y coordinación de los Sum

El cliente envía `FRUIT_RECORD` y termina con `END_OF_RECORDS`. Gateway confirma ambos con `ACK`. Al finalizar el procesamiento devuelve `FRUIT_TOP`, que el cliente confirma. Los datos y el EOF usan `[[clientId, isEOF, messageCount, coordinatorId], [[fruta, cantidad], ...]]`. El EOF inicial incluye en `messageCount` el total de mensajes registrados para ese cliente en Gateway.

Los Sum comparten la cola de entrada y se reparten sus mensajes. Por eso recibir el EOF en una instancia no significa que las demás hayan terminado. Para resolverlo, cada Sum mantiene contadores locales y un estado por cliente: `SENDING` durante la acumulación, `UPDATING` durante la coordinación del cierre y `END` al terminar.

Los controles entre los Sum usan `[tipo, senderId, clientId, messageCount]` y viajan por un exchange separado. Cada instancia escucha por un Exchange usando `all` y su ID como clave. Esto permite que reciban broadcasts y mensajes individuales.

| Mensaje recibido por el Sum | Adaptación del Sum para ese cliente |
| --- | --- |
| Datos (`isEOF=false`) | En `SENDING`, acumula por fruta con `FruitItem.Sum`, incrementa su contador y flushea cada 10 mensajes. En `UPDATING`, flushea después de cada mensaje y envía su contador mediante `COUNT_UPDATE`. Si es coordinador, verifica directamente si puede finalizar. |
| EOF del Gateway (`isEOF=true`) | Se convierte en coordinador, guarda el total esperado, pasa a `UPDATING`, publica los parciales pendientes y difunde `EOF_NOTICE`. |
| `EOF_NOTICE` (0) | Si proviene de otro Sum, registra al emisor como coordinador del cliente, pasa a `UPDATING`, flushea lo acumulado y responde `EOF_ACK` con su contador local. Ignora su propio aviso. |
| `EOF_ACK` (1) | El coordinador en `UPDATING` incorpora el contador del emisor y verifica si se alcanzó el total esperado. |
| `COUNT_UPDATE` (2) | El coordinador actualiza el contador del emisor y vuelve a verificar el cierre. Conserva el máximo recibido por instancia: los valores son acumulados. |
| `COMPlETE` (3) | Los otros Sum marcan `END` y eliminan el estado del cliente. El coordinador hace esa limpieza al terminar de publicar el cierre. |

## Escalabilidad

- **Clientes:** los IDs separan registros, contadores y estados. El coordinador se elige por cliente y puede ser un Sum diferente en cada caso. La memoria crece con los clientes concurrentes; el Sum elimina su estado al finalizar, mientras el Aggregation y el Join conservan marcadores de finalización.
- **Volumen de datos:** el cliente lee progresivamente y el Sum limita sus lotes a 10 mensajes. El Aggregation y el Join almacenan frutas distintas por cliente. Más registros de las mismas frutas aumentan el procesamiento sin aumentar proporcionalmente los registros; más frutas distintas sí aumentan la memoria.
- **Cantidad de controles:** más Sum hacen load balancing de la cola. Más Aggregation reparten las frutas mediante el hash. Los datos son enviados a un solo receptor, los broadcast se reservan para coordinar. Los mensajes de coordinación crecen con las instancias y las actualizaciones posteriores al aviso EOF. Gateway, RabbitMQ y el único Join siguen siendo puntos compartidos; no se implementa redistribución de particiones durante la ejecución.

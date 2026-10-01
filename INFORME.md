# Multiples clientes
En el message handler del gateway, dado que hay un objeto nuevo por cliente, en la inicialización genero la identidad del cliente, un identificador único.
Ahora, el sistema tendrá que implementar mensajes que incorporen este id de cliente, tanto para los mensajes de fruta/cantidad, como los EOF.
En vez de manejar un diccionario de frutas/cantidad, ahora se utiliza un diccionario donde las claves son los clientes, y el valor, las frutas/cantidad
Aggregation calcula un top independiente para cada cliente, y Sum espera a recibir el EOF del cliente antes de frowardear los datos al Aggregator

# Multiples sums
Debido a que del gateway sale solo un EOF por cliente, y hay varios SUMS, hay que coordinarlos para que cuando uno reciba el EOF, los otros se enteren.
Se implementan colas de control entre los sums, tal que cuando se recibe un EOF del gateway, el sum elegido al azar que extrajo de la input queue, ahora hace broadcast a todos, (y a si mismo) de un mensaje de control EOF.
Al recibirlo, cada sum forwardea al aggregator toda la info que tenga de ese cliente. Ahora el aggregator debe esperar a N_SUMS EOF de cada cliente. Esta solución puede dar lugar a condiciones de carrera, por lo que fue corregida mas adelante.

# Multiples aggregators
Para evitar procesamiento redundante, reparto la carga mediante un hash a partir de cliente/fruta. De esta forma se elige de forma pseudoaleatoria a un aggregator para que se ocupe de esa combinación, en vez de que todos hagan todo.
Antes, el joiner también recibía varias veces el mismo resultado, lo que daba a error porque esperaba a uno solo.

# Corrección múltiples sums
Cuando el hilo de control recibe un mensaje EOF, el hilo que maneja la input queue pudo haber estado procesando un mensaje.
Peor aun, si el prefetch definido en la implementación del middleware es != 1, se pudieron haber tomado varios mensajes, mientras que el control ya está por forwardear todo al aggregator.
Para resolver esto, elimino la control queue, y utilizo un TOKEN EOF en la input queue. De esta forma, en el SUM hay un solo hilo manejando una sola cola, eliminando la posible condicion de carrera.
Al recibir el EOF, si no está el nombre de la réplica del SUM en la lista, forwardea todo lo que tiene al aggregator, y reencola al final de la input queue el mismo token ,pero ahora con su nombre.
Al ser el EOF direccionado de forma aleatoria, puede que se lo envie a un SUM que ya lo habia recibido. Entonces lo vuelve a reenviar sin más.
En un punto, todos habrán recibido el EOF.
La cantidad de EOFs que tienen que ser enviados por cada cliente escala de forma N_SUMS x log(N_SUMS), como el problema del *coupon collector* .
Ejemplo, si hay 100 sums, en promedio se tendría que reenviar el token aprox 500 veces (100 * H_99)

---
layout: post
title:  "Procesamiento oportuno (y con estado) con Apache Beam"
date:   2017-08-28 00:00:01 -0800
excerpt_separator: <!--more-->
categories: blog
authors:
  - klk
---
<!--
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

En una [publicación anterior del blog]({{ site.baseurl }}/blog/2017/02/13/stateful-processing.html),
introduje las bases del procesamiento con estado (Stateful processing) en Apache
Beam, enfocado en la adición de estado al procesamiento por elemento. También
llamado procesamiento _oportuno_ (Timely processing), complementa el procesamiento
con estado en Beam dejándote establecer temporizadores para solicitar una
notificación (con estado) en algún punto en el futuro.

¿Qué puedes hacer con los temporizadores en Beam? Aquí hay unos ejemplos:

 - Puedes generar datos almacenados en búfer con un estado después de cierta
   cantidad de tiempo de procesamiento.
 - Puedes realizar una acción especial cuando un marcador estima que ha
   recibido todos los datos hasta un punto específico en el momento del evento.
 - Puedes crear flujos de trabajo con tiempos de espera que alteran el estado y
   emiten una salida en respuesta a la falta de una entrada adicional durante un
   período de tiempo.

Estas son sólo algunas posibilidades. El estado y los temporizadores juntos
forman un poderoso paradigma de programación para que el control detallado
exprese una gran variedad de flujos de trabajo. El procesamiento con estado y
oportuno en Beam es portable a través de los motores de procesamiento de datos
y se integra con el modelo unificado de Beam de ventanas de tiempo de eventos
tanto en transmisión como en procesamiento por lotes.

<!--more-->

## ¿Qué es procesamiento con estado y oportuno?

En mi publicación anterior, desarrollé una comprensión del procesamiento con
estado en gran medida contrastando con los combinadores asociativos, conmutativos.
En esta publicación, enfatizaré una perspectiva que mencioné solo brevemente: ese
procesamiento elemental con acceso al estado y temporizadores por clave y
ventana representa un patrón fundamental para el cálculo "vergonzosamente
paralelo", distinto de los otros en Beam.

De hecho, el cálculo estadístico y oportuno es el patrón computacional de bajo
nivel que subyace a los demás. Precisamente porque es de nivel inferior, te
permite controlar realmente tus cálculos para desbloquear nuevos casos de uso y
nuevas eficiencias. Esto incurre en la complejidad de administrar manualmente
tu estado y temporizadores, ¡no es magia! Veamos nuevamente los dos
patrones computacionales primarios en Beam.

### Procesamiento de elementos (ParDo, Map, etc)

El más elemental patrón paralelo es usar un grupo de computadoras para aplicar
la misma función a cada elemento entrante desde una colección masiva. En Beam,
el procesamiento por elemento como el ya mencionado es expresado con un basico
`ParDo` - Analogo a "Map" de MapReduce - que es como una mejora de "map",
"flatMap", etc, de programación funcional.

El siguiente diagrama ilustra el procesamiento por elemento. Los elementos de
entrada son cuadrados, los de salida son triangulos. Los colores de los
elementos representan sus claves, que serviran más adelante. Cada elemento de
entrada mapea la correspondiente salida de elemento(s) completamente independiente.
El procesamiento puede ser distribuido a través de computadoras de cualquier forma,
produciendo un paralelismo esencialmente ilimitado.

<img class="center-block"
    src="{{ site.baseurl }}/images/blog/timely-processing/ParDo.png"
    alt="ParDo ofrece paralelismo limitado"
    width="600">

Este patron es obvio, existe en todos los paradigmas de datos en paralelo, y
tiene una implementación con estado simple. Cada elemento de entrada puede ser
procesado independientemente o en grupos arbitrarios. El balanceo del trabajo
entre computadoras es actualmente la parte dificil, y puede ser dirigido por la
división, estimación de progreso, robo de trabajo, etc.

### Agregación (Combine, Reduce, GroupByKey, etc.) por clave (y ventana)

El otro embarazoso patron de diseño paralelo en el corazón de Beam es la agregación
(y ventana). Elementos compartiendo una clave son agrupados y luego combinados
usando algún operador asociativo y conmutativo. En Beam esto expresado como un
`GroupByKey` o `Combine.perKey`, y corresponde a la mezcla y "Reducción" de
MapReduce. Es a veces útil pensar en `Combine` por clave como una operación
fundamental, y `GroupByKey` a secas como un combinador que simplemente concatena
elementos de entrada. El patrón de comunicación para los elementos de entrada es
el mismo, modulo con algúnas optimizaciones posibles para `Combine`.

En la ilustración de aqui, recordando que el color de cada elemento representa
la clave. Por lo que todos los cuadrados rojos son enviados a la misma ubicación
donde son agrupados y el triangulo rojo es la salida. Del mismo modo para
los cuadrados amarillos y verdes, etc. En una aplicacción real, puedes tener
millones de claves, por lo que el paralelismo es todavía masivo.

<img class="center-block"
    src="{{ site.baseurl }}/images/blog/timely-processing/CombinePerKey.png"
    alt="Uniendo elementos por clave y luego combinandolos"
    width="600">

EL motor de procesamiento de datos subaycente, en algún nivel de abstracción,
usará el estado para realizar esta agrupación en todos los elementos que llegan
para una misma clave. En particular, en una ejecución de streaming, el proceso de
agrupación puede necesitar esperar por más datos que lleguen o para que un
marcador estime que toda la entrada para una ventana de tiempo de evento esta
completa. Esto requiere alguna forma de almacenar la agrupación intermedia
entre los elemntos de entrada, así como una forma de recibir una notificación
cuando es hora de emitir el resultado. Como resultado, la _ejecución_ de la
agrupación por clave en un motor de procesamiento de un stream fundamentalmente
implica el estado y los temporizadores.

Sin embargo, _tú_ codigo es solo una expresión declarativa del operador de
agrupación. El runner puede escoger una variedad de formas de ejecutar su
operador. Profundice esto más en detalle en [mí publicación anterior centrada en el estado]({{ site.baseurl }}/blog/2017/02/13/stateful-processing.html). Como
no observa los elementos en ningún orden definido, ni manipula el estado mutable
o los temporizadores directamente, no lo llamo procesamiento de estado ni
oportuno.

### Procesamiento puntual, por estado con clave y ventana.

`ParDo` y `Combine.perKey` son patrones estandar para el paralelismo que viene
de decadas atras. Cuando estos se implementan en un motor de procesamiento de
datos distribuidos a gran escala, podemos resaltar unas pocas caracteristicas
que son particularmente importantes.

Consideremos estas caracteristicas de `ParDo`:

 - Escribes código que es monotarea para procesar un elemento.
 - Los elementos son procesados en un orden arbitrario sin dependecias o 
   interacción entre el procesamiento de datos.

Y estas caracteristicas para `Combine.perKey`:

 - Los elementos para una clave y ventana común se unen.
 - Un operador definido por el usuario es aplicado a esos elementos.

Combinando algunas de las características del mapeo paralelo no restringido y
la combinación por clave y ventana, podemos discernir un megaprimitivo a partir
del cual construimos un procesamiento con estado y oportuno:

 - Los elementos para una clave y ventana común se unen.
 - Los eleemntos son procesados en un orden arbitrario.
 - Escribes codigo que es monotarea para procesar un elemento o temporizador,
   posiblemente para accesar al estado o configurar temporizadores.

El la siguiente ilustración, los cuadros rojos son unidos y procesados uno a uno
con un estado, temporizador y `DoFn`. Como cada elemento es procesado, el `DoFn`.
tiene que accesar al estado (el cilindro coloreado en partes a la derecha) y
puede establecer temporizadores para recibir notificaciones (los relojes
coloreados a la izquierda).

<img class="center-block"
    src="{{ site.baseurl }}/images/blog/timely-processing/StateAndTimers.png"
    alt="Uniendo elementos por clave y luego por tiempo, procesamiento con estado"
    width="600">

Así que es la notación abstracta de un estado por-clave-y-ventana, procesado
oportunamente en Apache Beam. Ahora veamos como se ve el codigo escrito que
accesa al estado, establece temporizadores y recibe notificaciones.

## Ejemplo: RPC por lotes

Para demostrar el procesamiento con estado y con temporizadores, trabajaremos
con un ejemplo concreto, con código.

Supongamos que estas escribiendo un sistema para analizar eventos. Tienes una
tonelada de datos llegando y necesitas enriquecer cada evento con RPC a un sistema
externo. No puedes simplemente emitir un RPC por evento. Esto no solo sería
terrible para el rendimiento, sino que probablemente también influira en su cuota
con el sistema externo. Así que te gustaría tomar un número de eventos, hacer un
RPC para todos, y entonces enviar a la salida todos los eventos enriquecidos.

### Estado

Configuremos el estado que necesitamos para rastrear lotes de elementos. Con
cada elemento entrante, escribiremos el elemento en el búfer mientras rastreamos
el número de elementos que tenemos en el búfer. Aquí estan las celdas de estado
en código:

```java
new DoFn<Event, EnrichedEvent>() {

  @StateId("buffer")
  private final StateSpec<BagState<Event>> bufferedEvents = StateSpecs.bag();

  @StateId("count")
  private final StateSpec<ValueState<Integer>> countState = StateSpecs.value();

  … TBD … 
}
```

```py
# El estado y los temporizadores aún no son soportados en el SDK Python de Beam
# Sigue https://issues.apache.org/jira/browse/BEAM-2687 para conocer de nuevas actualizaciones.
```

Llendo a tráves del código, tenemos:

 - La celda de estado `"buffer"` es una bolsa desordenada de eventos almacenados
   en búfer.
 - La celda de estado `"count"` rastrea cuantos eventos han sido almacenados en
   búfer.

Siguiendo, como una recapitulación de la lectura y escritura de estado,
escribiremos nuestro método `@ProcessElement`. Escogeremos un limite sobre el
tamaño del búfer, `MAX_BUFFER_SIZE`. Si nuestro búfer alcanza ese tamaño,
realizaremos un solo RPC para enriquecer todos los eventos y generar una salida.

```java
new DoFn<Event, EnrichedEvent>() {

  private static final int MAX_BUFFER_SIZE = 500;

  @StateId("buffer")
  private final StateSpec<BagState<Event>> bufferedEvents = StateSpecs.bag();

  @StateId("count")
  private final StateSpec<ValueState<Integer>> countState = StateSpecs.value();

  @ProcessElement
  public void process(
      ProcessContext context,
      @StateId("buffer") BagState<Event> bufferState,
      @StateId("count") ValueState<Integer> countState) {

    int count = firstNonNull(countState.read(), 0);
    count = count + 1;
    countState.write(count);
    bufferState.add(context.element());

    if (count > MAX_BUFFER_SIZE) {
      for (EnrichedEvent enrichedEvent : enrichEvents(bufferState.read())) {
        context.output(enrichedEvent);
      }
      bufferState.clear();
      countState.clear();
    }
  }

  … TBD … 
}
```

```py
# El estado y los temporizadores aún no son soportados en el SDK Python de Beam
# Sigue https://issues.apache.org/jira/browse/BEAM-2687 para conocer de nuevas actualizaciones.
```

Aquí esta una ilustración para acompañar el código:

<img class="center-block"
    src="{{ site.baseurl }}/images/blog/timely-processing/BatchedRpcState.png"
    alt="Batching elements in state, then performing RPCs"
    width="600">

 - La caja azul es el `DoFn`.
 - La caja amarilla dentro de él es el método `@ProcessElement`.
 - Cada evento de entrada es un cuadro rojo - este diagrama solo muestra la
   actividad para una sola clave, representada por el color rojo. Tú `DoFn`
   ejecutará el mismo flujo de trabajo en paralelo para todas las claves que son
   quiza los IDs de usuario.
 - Cada evento de entrada es escrito al búfer como un triangulo rojo,
   representando el hecho de que todavía puedes enviar al búfer más que solo
   una entrada sin formato, aunque este código no lo haga.
 - El servicio externo es dibujado como una nube. Cuando hay suficientes eventos
   en el búfer, el método `@ProcessElement` lee los eventos de un estado y emite
   un único RPC.
 - Cada salida de eventos enriquecidos es dibujado como un circulo rojo. Para
   los consumidores de esta salida, parece solo como una operación de elementos
   en paralelo.

Hasta ahora, tenemos solo el estado usado, pero no los temporizadores. Puedes
haber avisado que hay un problema - usualmente serán datos perdidos en el búfer.
Si no llegan más entradas, esos datos nunca serán procesados. En Beam, cada
ventana tiene algún punto en el momento del evento cuando cualquier entrada es
considerada que ha llegado muy tarde y es descartada. En ese punto, decimos que
la ventana ha "expirado". Dado que no pueden llegar más entradas para acceder al
estado de esa ventana, el estado también se descarta. Para nuestro ejemplo,
debemos asegurarnos de que todos los eventos restantes se generen cuando la
ventana expire.

### Temporizadores de evento

Un temporizador de evento solicita una notificación cuando la marca para una
`PCollection` de entrada alcanza algún umbral. En otras palabras, puedes usar un
temporizador de eventos para llevar a cabo una acción en un momento especifico
en el tiempo del evento - un punto particular de integridad para un `PCollection`
- como cuando una ventana expira.

Por ejemplo, sumemos un temporizador de evento que cuando la ventana expire, los
eventos restantes en el búfer serán procesados.

```java
new DoFn<Event, EnrichedEvent>() {
  …

  @TimerId("expiry")
  private final TimerSpec expirySpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

  @ProcessElement
  public void process(
      ProcessContext context,
      BoundedWindow window,
      @StateId("buffer") BagState<Event> bufferState,
      @StateId("count") ValueState<Integer> countState,
      @TimerId("expiry") Timer expiryTimer) {

    expiryTimer.set(window.maxTimestamp().plus(allowedLateness));

    … same logic as above …
  }

  @OnTimer("expiry")
  public void onExpiry(
      OnTimerContext context,
      @StateId("buffer") BagState<Event> bufferState) {
    if (!bufferState.isEmpty().read()) {
      for (EnrichedEvent enrichedEvent : enrichEvents(bufferState.read())) {
        context.output(enrichedEvent);
      }
    }
  }
}
```

```py
# El estado y los temporizadores aún no son soportados en el SDK Python de Beam
# Sigue https://issues.apache.org/jira/browse/BEAM-2687 para conocer de nuevas actualizaciones.
```

Descompongamos en piezas este fragmento:

 - Declaramos un temporizador de evento `@TimerId("expiry")`. Usaremos el
   identificador `"expiry"` para identificar el temporizador y configurar el
   tiempo para la notificación tan pronto como se reciba la notificación.

 - La variable `expiryTimer`, anotada con `@TimerId`, es establecida para el valor
   `TimerSpecs.timer(TimeDomain.EVENT_TIME)`, indicando que queremos una
   notificación de acuerdo a la marca del temporizador de evento de los elementos
   de entrada.

 - En el elemento `@ProcessElement` tenemos anotado un parametro
   `@TimerId("expiry") Timer`. El runner Beam automaticamente provee a este
   parametro `Timer` de lo que podemos configurar (y restablecer) al temporizador.
   No es una gran carga resetear un temporizador de forma repetida, así que
   simplemente lo hacemos en cada elemento.

 - Definimos el método `onExpiry`, anotado con `@OnTimer("expiry")`, que realiza
   un RPC de enriquecimiento del evento final y genera una salida de resultados.
   EL runner Beam entrega la notificación a este método para coincidir su
   identificador.

Ilustrando esta lógica, tenemos el siguiente diagrama:

<img class="center-block"
    src="{{ site.baseurl }}/images/blog/timely-processing/BatchedRpcExpiry.png"
    alt="Batched RPCs with window expiration"
    width="600">


Los métodos `@ProcessElement` y `@OnTimer("expiry")` realizan el mismo acceso al
estado en búfer, realizan el mismo RPC por lotes y generan una salida de
elementos enriquecidos.

Ahora, si estamos ejecutamos esto en un streaming de tiempo real, es posible que
tengamos una latencia ilimitada para datos en búfer en particular. Si la marca
esta avanzando muy lentamente, o el momento de ventanas de eventos que son
escogidas son demasiado grandes, entonces puede pasar mucho tiempo para que la
salida sea emitida ya sea con los elementos suficientes o con la expiración de
la ventana. Podemos también usar temporizadores para limitar la cantidad de
tiempo de reloj, tiempo de procesamiento, antes de que procesemos elementos del
búfer. Podemos escojer alguna cantidad razonable de tiempo para que aunque
estemos emitiendo RPCs que no son tan grandes, sean aún menos RPCs para evitar
acabar con nuestra couta del servicio externo.

### Temporizadores de procesamiento

Un temporizador en tiempo de procesamiento (el tiempo que pasa mientras su
tubería es ejecutada) es intuituvamente simple: quieres esperar una cierta
cantidad de tiempo y entonces recibir una notificación.

Para poner el toque final a nuestro ejemplo, estableceremos el tiempo de
procesamiento del temporizador tan pronto como cualquier dato sea almacenado
en búfer. Registramos si o no el temporizador ha sido establecido así que no
se reestablecera continuamente. Cuando un elemento llega, si el temporizador
no ha sido establecido, entonces lo estableceremos para el momento actual
sumandole `MAX_BUFFER_DURATION`. Despues que el tiempo de procesamiento haya
transcurrido, una notificación se lanzara y enriquecera y emitira cualquier
elemento en búfer.

```java
new DoFn<Event, EnrichedEvent>() {
  …

  private static final Duration MAX_BUFFER_DURATION = Duration.standardSeconds(1);

  @TimerId("stale")
  private final TimerSpec staleSpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

  @ProcessElement
  public void process(
      ProcessContext context,
      BoundedWindow window,
      @StateId("count") ValueState<Integer> countState,
      @StateId("buffer") BagState<Event> bufferState,
      @TimerId("stale") Timer staleTimer,
      @TimerId("expiry") Timer expiryTimer) {

    boolean staleTimerSet = firstNonNull(staleSetState.read(), false);
    if (firstNonNull(countState.read(), 0) == 0) {
      staleTimer.offset(MAX_BUFFER_DURATION).setRelative());
    }

    … same processing logic as above …
  }

  @OnTimer("stale")
  public void onStale(
      OnTimerContext context,
      @StateId("buffer") BagState<Event> bufferState,
      @StateId("count") ValueState<Integer> countState) {
    if (!bufferState.isEmpty().read()) {
      for (EnrichedEvent enrichedEvent : enrichEvents(bufferState.read())) {
        context.output(enrichedEvent);
      }
      bufferState.clear();
      countState.clear();
    }
  }

  … same expiry as above …
}
```

```py
# El estado y los temporizadores aún no son soportados en el SDK Python de Beam
# Sigue https://issues.apache.org/jira/browse/BEAM-2687 para conocer de nuevas actualizaciones.
```

Aquí esta una ilustración del código final:

<img class="center-block"
    src="{{ site.baseurl }}/images/blog/timely-processing/BatchedRpcStale.png"
    alt="Batching elements in state, then performing RPCs"
    width="600">

Recapitulando la totalidad de la lógica:

 - A medida que llegan los elementos `@ProcessElement` son almacenados en búfer
   en estado.
 - Si el tamaño del búfer excede un máximo, los eventos son enriquecidos y una
   salida es generada.
 - Si el tamaño del búfer se llena muy lentamente y los eventos quedan obsoletos
   antes de alcanzar el maximo, un temporizador causa una notificación que
   enriquece los elementos del búfer y genera una salida con ello.
 - Finalmente, en cuanto cualquier ventana es expirada, cualquiera de los eventos
   en búfer en esa ventana son procesados y se genera una salida anterior al
   estado para que esa ventana sea descartada. 

Al final, tenemos un ejemplo completo que usa el estado y los temporizadores para
explicitar la administración de los detalles de bajo nivel de una transformación
sensible al rendimiento en Beam. Como agreguemos más y más caracteristicas,
nuestro `DoFn` en realidad se hace muy grande. Eso es una caracteristica normal
del procesamiento oportuno y con estado. Realmente estás investigando y
administrando una gran cantidad de detalles que se manejan automáticamente
cuando expresas tu lógica utilizando las API de nivel superior de Beam. Lo que
obtienes de esto es un esfuerzo adicional que es la capacidad de abordar casos
de uso y aceptar eficiencias que no pueden ser posibles de otra forma.

## Estado y temporizadores en el Modelo Unificado de Beam

El modelo unificado de Beam para eventos de tiempo a traves de streaming y
procesamiento por lostes tiene nuevas implicaciones para estado y temporizadores.
Usualmente, no necesitas hacer nada para que tú `DoFn` oportuno y con estado
funcione bien en el modelo de Beam. Pero te ayudará a ser cuidadoso de las
consideraciones siguientes, especialmente si has usado caracteristicas similares
antes de haber usado Beam.

### Eventos de tiempo en ventana "Solo funciona"

Una de las razones de ser de Beam es el correcto procesamiento de cada orden
de datos de eventos. La solución de Beam para cada orden de datos es eventos de
tiempo en ventana, donde las ventanas en tiempo de evento lanzan los resultados
correctos sin importar que ventana elija un usuario o en que orden entren los
eventos.

Si escribes una transformación oportuna y con estado, debería funcionar sin
importar como circundante elija la ventana del tiempo de evento. Si la tubería
elije ventanas fijas de una hora (a veces llamado ventanas giratorias) o
ventanas de 30 minutos que se deslizan cada 10 munitos, la transformación
oportuna y con estado debería funcionar correctamente transparente.

<img class="center-block"
    src="{{ site.baseurl }}/images/blog/timely-processing/WindowingChoices.png"
    alt="Two windowing strategies for the same stateful and timely transform"
    width="600">

Esto funciona automaticamente en Beam, porque el estado y los temporizadores
son particionados por clave y ventana. Dentro de cada clave y ventana, el
procesamiento oportuno y con estado es esencialmente independiente. Como un
beneficio agregado, el pase de eventos de tiempo (también conocido como avance
de marca) permite una liberación automatica de estado inalcanzable cuando una
ventana expira, así que no tienes que preocuparte frecuentemente de desalojar
un estado anterior.

### Procesamiento historico y en tiempo real unificado

Un segundo principio del modelo de semantica Beam es que el procesamiento debe
ser unificado entre lotes y streaming. Un importante caso de uso para esta
unificación esla habilidad de aplicar la misma lógica al stream de eventos en
tiempo real y el almacenamiento archivado de los mismos eventos.

Una carcacteristica común de los datos archivados es que pueden llegar
radicalmente sin un orden. La fragmentación de los archivos a menudo resultan en
un orden totalemente diferente para el procesamiento de eventos llegando en
casi tiempo real. Los datos también estarán disponibles y por lo tanto, entregados
instantaneamente desde el punto de vista de su tubería. Si corre experimentos
sobre datos pasados o representando resultados pasados para arreglar
un bug de procesamiento de datos, es criticamente importante que su lógica de
procesamiento sea aplicable eventos archivados tan fácilmente como datos casi en
tiempo real.

<img class="center-block"
    src="{{ site.baseurl }}/images/blog/timely-processing/UnifiedModel.png"
    alt="Unified stateful processing over streams and file archives"
    width="600">

Es (deliberadamente) posible escribir un DoFn oportuno y con estado que entregue
resultados que dependen en el oren o el tiempo de entrega, así que en este sentido
hay una carga adicional para tí, el autor de `DoFn`, para garantizar que este
indeterminismo se encuentre dentro de los permisos documentados.

## ¡Ve y usalo!

Finalizaré esta publicación de la misma manera en como lo hice en el anterior.
Espero que pruebes el procesamiento oportuno y con estado de Beam. Si se habren
nuevas posibilidades para tí, ¡Eso es grandioso! Si no, queremos saber sobre eso.
Debido a que esta es una nueva caracteristica, por favor revisa la [matriz de disponibildad de capacidades]({{ site.baseurl }}/documentation/runners/capability-matrix/) para ver el nivel de soporte para tú o tus backends
preferidos de Beam.

Y por favor uneté a nuestra comunidad Beam en 
[user@beam.apache.org]({{ site.baseurl }}/get-started/support) y siguenos en Twitter
[@ApacheBeam](https://twitter.com/ApacheBeam).

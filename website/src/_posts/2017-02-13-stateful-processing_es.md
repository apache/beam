---
layout: post
title:  "Procesamiento con estado en Apache Beam"
date:   2017-02-13 00:00:01 -0800
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

Beam le permite procesar datos ilimitados, fuera de orden y de escala global
con canalizaciones portátiles de alto nivel. El procesamiento con estado
(Stateful processing) es una nueva característica del modelo Beam que expande
las capacidades de Beam, desbloqueando nuevos casos de uso y nuevas eficiencias.
En esta publicación, lo guiaré a través del procesamiento con estado en Beam:
cómo funciona, cómo encaja con las otras características del modelo Beam, para
qué podría usarlo y cómo se ve en el código.

<!--more-->

> **Advertencia: nuevas características en progreso!**: Este es un aspecto muy
> nuevo del modelo Beam. A los runners se les sigue agregando soporte. Puede
> probarlo hoy en varios runners, pero revise la [matriz de disponibilidad de capacidades del runner]({{ site.baseurl }}/documentation/runners/capability-matrix/)
> para ver el estado actual de cada runner.

Primero, una recapitulsción rapída: En Beam, una _tubería_ (pipeline) de
procesamiento de Big Data es un grafó aciclico y dirigido de operaciones
paralelas llamadas _`PTransforms`_ procesando datos desde _`PCollections`_.
Voy a estó a tráves de la siguiente ilustración:

<img class="center-block" 
    src="{{ site.baseurl }}/images/blog/stateful-processing/pipeline.png" 
    alt="Una tubería Beam - PTransforms son cajas - PCollections son flechas" 
    width="300">

Los cuadros son `PTransforms` y los bordes representan los datos en
`PCollections` que fluyen de un `PTransform` al siguiente. Una `PCollection`
puede ser _limitada_ (lo que significa que es finita y usted lo sabe) o
_ilimitada_ (lo que significa que no sabe si es o no es finita - básicamente,
es como un flujo de datos entrante que puede o no terminar alguna vez). Los
cilindros son las fuentes de datos y bajan por los bordes de su tubería, como
las colecciones limitadas de archivos log o la transmisión de datos
ilimitados sobre un tema de Kafka. Esta publicación no se trata de
fuentes o sumideros, sino de lo que ocurre entre ellos: el procesamiento de
datos.

Hay dos bloques principales de construcción para el procesamiento de sus datos
en Beam:  _`ParDo`_, para realizar una operación en paralelo en todos los
elementos, y _`GroupByKey`_ (y el cercanamente relacionado `CombinePerKey` del
cual hablare muy pronto) para la agregación de elementos que tienen asignada la
misma clave. En la imagen siguiente (que aparece en muchas de nuestras
presentaciones) el color indica la clave del elemento. Así, la transformación
`GroupByKey`/`CombinePerKey` une todos los cuadros verdes para producir un único
elemento de salida.

<img class="center-block" 
    src="{{ site.baseurl }}/images/blog/stateful-processing/pardo-and-gbk.png"
    alt="ParDo y GroupByKey/CombinePerKey: 
        Elementwise versus computos de agregación"
    width="400">

Pero no todos los casos de uso son facilmente expresados como tuberias de
transformaciones simples `ParDo`/`Map` y `GroupByKey`/`CombinePerKey`. El tema
de esta publicación es una nueva extensión del modelo de programación de
Beam: **operación por elemento aumentada con un estado mutable**.

<img class="center-block" 
    src="{{ site.baseurl}}/images/blog/stateful-processing/stateful-pardo.png"
    alt="ParDo con estado - procesamiento secuencial por clave con un estado
    persistente"
    width="300">

En la ilustración anterior, ParDo ahora tiene un poco de estado duradero y
consistente a un lado, que se puede leer y escribir durante el procesamiento de
cada elemento. El estado se divide por clave, por lo que se dibuja como
teniendo secciones separadas para cada color. También está particionado por
ventana, pero pensé que el escoces,
<img src="{{ site.baseurl }}/images/blog/stateful-processing/plaid.png"
    alt="Un cilindro de almacenamiento escoces" width="20">
un cilindro de almacenamiento a cuadros sería un poco mejor :-). Hablaré
acerca de por qué el estado se divide de esta manera un poco más tarde, a
través de mi primer ejemplo.

Para el resto de esta publicación, describiré esta nueva característica de Beam
en detalle: cómo funciona a un alto nivel, en qué se diferencia de las
características existentes, cómo asegurarse de que aún sea masivamente
escalable. Después de esa introducción a nivel de modelo, veré un ejemplo
sencillo de cómo lo usa en el SDK de Java de Beam.

## ¿Cómo funciona el procesamiento con estado en Beam?

La lógica de procesamiento de su transformación `ParDo` se expresa a través de
la `DoFn` que se aplica a cada elemento. Sin aumentos de estado, una función
`DoFn` mayoritariamente pura de entradas a una o más salidas, que corresponde
al Mapper en un MapReduce. Con el estado, un `DoFn` tiene la capacidad de
acceder al estado mutable persistente al procesar cada elemento de entrada.
Considera esta ilustración:

<img class="center-block" 
    src="{{ site.baseurl}}/images/blog/stateful-processing/stateful-dofn.png"
    alt="DoFn con estado - 
        el runner controla la entrada pero el DoFn controla almacenamiento y
        entrada"
    width="300">

Lo primero que se debe tener en cuenta es que todos los datos - los pequeños
cuadrados, círculos y triángulos - están en rojo. Esto es para ilustrar que el
procesamiento con estado ocurre en el contexto de una sola clave - todos los
elementos son pares clave-valor con la misma clave. Las llamadas desde el
runner Beam elegido al `DoFn` aparecen en color amarillo, mientras que las
llamadas desde el `DoFn` al runner están en color púrpura:

 - El runner invoca el metodo `@ProcessElement` de `DoFn` en cada elemento para
   una clave+ventana.
 - El `DoFn` lee y escribe un estado - las flechas curvadas hacia/desde el
   almacenamiento a un lado.
 - El `DoFn` emite una salida (o una salida al lado) al runner como de
  costumbre a través de `ProcessContext.output` (resp. `ProcessContext.sideOutput`).

En este nivel tan alto, es muy intuitivo: en su experiencia de programación,
es probable que en algún momento haya escrito un bucle sobre elementos que
actualizan algunas variables mutables mientras realizan otras acciones. La
pregunta interesante es cómo encaja esto en el modelo Beam: ¿cómo se relaciona
con otras características? ¿Cómo se escala, ya que el estado implica alguna
sincronización? ¿Cuándo se debe usar en comparación con otras características? 

## ¿Cómo encaja el procesamiento con estado en modelo Beam?

Para ver donde encaja el procesamiento con estado dentro del modelo Beam,
considere otra forma en la que puede conservar algun "estado" mientras son
procesados muchos elementos: CombineFn. En Beam, puede escribir
`Combine.perKey(CombineFn)` con Java o Python para aplicar una operación
de acumulación conmutativa asociativa en todos los elementos con una clave
común (y una ventana).

Aquí esta un diagrama ilustrando las bases de un `CombineFn`, la forma más
simple en que un runner puede invocarlo por clave para construir un acumulador
y extraer una salida del un acumulador final:

<img class="center-block"
    src="{{ site.baseurl }}/images/blog/stateful-processing/combinefn.png"
    alt="CombineFn - el runner controla la entrada, almacenamiento, y la salida"
    width="300">

Al igual que con la ilustración de estado `DoFn`, todos los datos están
coloreados en rojo, ya que este es el proceso de Combinar para una sola clave.
Las llamadas al método ilustrado son de color amarillo, ya que todas están
controladas por el runner: el runner invoca `addInput` sobre cada método para
agregarlo al acumulador actual.

 - El runner mantiene el acumulador cuando es eljido.
 - El runner llama `extractOutput` cuando esta listo para emitir un elemento
   de salida.

En este punto, el diagrama de `CombineFn` se parece mucho al diagrama de
`DoFn` con estado. En la practica el flujo de datos es, de hecho, bastante
similar. Pero hay importantes diferencias, incluso así:

 - El runner aquí controla todas las invocaciones y almacenamiento. Tú no
   decides cuando o como el estado es mantenido, cuando un acumulador es
   descartado (basandose en un disparador) o cuando la salida es extraida de un
   acumulador.
 - Solo puedes tener una pieza de estado - el acumulador. En un DoFn con estado
   solo puedes leer lo que necesitas saber y escribir solo lo que ha cambiado.
 - No tienes que extender las caracteristicas de `DoFn`, así como multiples
   salidas por entrada o las salidas de a un lado. (Eso podría ser simulado por
   un suficientemente complejo acumulador, pero no sería natural o eficiente.
   Algunas otras caracteristicas de `DoFn` como las entradas al lado y el
   acceso a la ventana crea una sensación perfecta de `CombineFn`)

Pero lo principal que `CombineFn` le permite a un runner hacer es
`mergeAccumulators`, la expresión concreta de la asociatividad de `CombineFn`.
Esto desbloquea algunas optimizaciones enormes: el runner puede invocar
multiples instancias de un `CombineFn` para un número de entradas y despues
combinarlas en una arquitectura clasica de divide y venceras como en esta imagen:

<img class="center-block" 
    src="{{ site.baseurl }}/images/blog/stateful-processing/combiner-lifting.png"
    alt="Agregación divide y venceras con un CombineFn"
    width="600">

El contrato de un `CombineFn` es que el resultado debería ser exactamente el
mismo, ya sea que el runner decida o no realmente hacer tal cosa o incluso
árboles más complejos con salidas de acceso rápido, etc.

Esta operación de mezcla no es (necesariamente) provista por un `DoFn` con
estado: el runner no puede ramificar libremente su ejecución y recombinar los
estados. Notar que los elementos de entrada aún son recibidos de forma
desordenada, así que `DoFn` debería ser insensible a la ordenación y
construcción pero eso no significa que la salida debe ser exactamente igual.
(hecho divertido y sencillo: si las salidas son siempre iguales actualmente,
entonces el `DoFn` es un operador asociativo y conmutativo)

Así que ahora puedes ver como un `DoFn` con estado se diferencia de un
`CombineFn`, pero quiero dar un paso atrás y extrapolarlo a una imagen de alto
nivel de cómo se relaciona el estado en Beam con el uso de otras funciones para
lograr los mismos objetivos o similares: en muchos casos, lo qué el
procesamiento con estado representa es una oportunidad para "ponerse bajo la
capucha" del paradigma funcional altamente determinista y altamente abstracto
de Beam y realizar una programación de estilo imperativo potencialmente no
determinista que es difícil de expresar de otra manera.

## Ejemplo: asignación de indice arbitraria pero consistente

Supongamos que deseas asignar un índice a cada elemento entrante para una
clave-y-ventana. No te importa cuáles son los índices, siempre y cuando sean
únicos y consistentes. Antes de sumergirme en el código para saber cómo hacer esto
en un SDK de Beam, repasaré este ejemplo desde el nivel del modelo. En las
imágenes, deseas escribir una transformación que mapea la entrada hacia la salida
de esta manera:

<img class="center-block" 
    src="{{ site.baseurl }}/images/blog/stateful-processing/assign-indices.png"
    alt="Asignación arbitraria pero unica de inidces a cada elemento"
    width="180">

El orden de los elementos A, B, C, D, E es arbitrario, por lo tanto, sus
índices asignados son arbitrarios, pero las transformaciones posteriores solo
deben estar bien con esto. No hay asociatividad ni conmutatividad en lo que se
refiere a los valores reales. La insensibilidad al orden de esta transformación
solo se extiende hasta el punto de garantizar las propiedades necesarias de la
salida: sin índices duplicados, sin huecos, y cada elemento obtiene un índice.

Conceptualmente, expresar esto como un bucle con estado es tan trivial como
puedas imaginar: el estado que debes almacenar es el siguiente índice.

 - A medida que entra un elemento, imprímelo junto con el siguiente índice.
 - Incrementear el indice.

Esto presenta una buena oportunidad para hablar sobre big data y paralelismo,
!porque el algoritmo en esos puntos no es paralelizable del todo!. Si quisieras
aplicar esta lógica a una `PCollection` entera, tendrías que procesar cada
elemento de la `PCollection` uno a la vez... esto es obviamente una mala idea.
El estado in Beam tiene un amplio ámbito de aplicación, por lo que la mayoría
de las veces una transformación `ParDo` con estado debería ser posible para que
un runner se ejecute en paralelo, aunque aún debe ser cuidadoso al respecto.

Una celda de estado en Beam tiene el alcance de un par clave+ventana. Cuando su
DoFn lee o escribe el estado por el nombre de `"index"`, en realidad está
accediendo a una celda mutable especificada por `"index"` _junto con_ la clave
y la ventana que se está procesando actualmente. Por lo tanto, cuando piense en
una celda de estado, puede ser útil considerar el estado completo de su
transformación como una tabla, donde las filas se nombran de acuerdo con los
nombres que usa en su programa, como `"index"`, y las columnas son pares
clave+ventana, como esto:

|               | (clave, ventana)<sub>1</sub> | (clave, ventana)<sub>2</sub> | (clave, ventana)<sub>3</sub> | ... |
|---------------|---------------------------|---------------------------|---------------------------|-----|
| `"index"`       | `3`                         | `7`                         | `15`                        | ... |
| `"fizzOrBuzz?"` | `"fizz"`                    | `"7"`                       | `"fizzbuzz"`                | ... |
| ...           | ...                       | ...                       | ...                       | ... |
{:.table}

(Si tiene un excelente sentido espacial, siéntase libre de imaginarlo como un
cubo donde las claves y las ventanas son dimensiones independientes)

Puedes proporcionar la oportunidad de paralelismo asegurándote de que la tabla
tenga suficientes columnas. Es posible que tenga muchas claves y muchas
ventanas, o que tenga muchas de solo una o la otra:

- Muchas claves en pocas ventanas, por ejemplo un calculo con estado de ventana
  globalmente que se le asignen claves a traves de un ID de usuario.
- Muchas ventanas sobre pocas claves, por ejemplo un calculo con estado de
  ventana fijo sobre una clave global.

Advertencia: todos los runners Beam de hoy trabajan en paralelo solo sobre la
clave.

Más a menudo su modelo mental de estado puede ser enfocado en solamente una
columna de la tabla, un solo par clva+ventana. Las interacciones entre columna
no ocurren directamente, por diseño.

## El estado en el SDK Java de Beam

Ahora que he hablado un poco sobre el procesamiento con estado en el modelo de
Beam y he analizado un ejemplo abstracto, me gustaría mostrarle cómo se escribe
el código de procesamiento con estado utilizando el SDK de Java de Beam. Aquí
está el código para un `DoFn` con estado que asigna un índice arbitrario pero
consistente a cada elemento en una base por clave-y-ventana:

```java
new DoFn<KV<MyKey, MyValue>, KV<Integer, KV<MyKey, MyValue>>>() {

  // Un celda de estado que posee un solo entero por clave+ventana
  @StateId("index")
  private final StateSpec<ValueState<Integer>> indexSpec = 
      StateSpecs.value(VarIntCoder.of());

  @ProcessElement
  public void processElement(
      ProcessContext context,
      @StateId("index") ValueState<Integer> index) {
    int current = firstNonNull(index.read(), 0);
    context.output(KV.of(current, context.element()));
    index.write(current+1);
  }
}
```

```py
# El estado y los temporizadores no son soportados aún dentro del SDK Python de Beam
# Mira este espacio!
```

Analizemos esto:

 - Lo primero a observar es la presencia de un par de anotaciones
   `@StateId("index")`. Esto indica que estás utilizando una celda de estado
   mutable llamada "índice" en este `DoFn`. El SDK Java de Beam, y de ahí su
   runner elegido, también tomará nota de estas anotaciones y las utilizará
   para conectar su DoFn correctamente.
 - EL primer `@StateId("index")` es anotado sobre un campo de tipo `StateSpec`
   (para una "especificación de estado"). Esto declara y configura la celda de
   estado. El parametro de tipo `ValueState` describe el tipo de estado que
   puedes tener de cada celda - `ValueState` almacena solo un valor. Toma en
   cuente que la especificación por si misma no es una celda de estado usable
   - necesitas que el runner proporcione eso durante la ejecución de la tubería.
 - Para especificar completamente una celda `ValueState`, necesitas proporcionar
   el codificador que el runner utilizará (según sea necesario) para serializar
   el valor que será almacenaras. Esto es la invocación `StateSpecs.value(VarIntCoder.of())`.
 - La segunda anotación `@StateId("index")` es sobre un parametro de tú método
   `@ProcessElement`. Esto indica el acceso para la celda ValueState que fue especificado anteriormente.
 - El estado es accesado en la forma más simple: `read()` para leerlo, y
   `write(newvalue)` para escribirlo.
 - Las otras caracteristicas de `DoFn` son disponibles en la forma usual - como
   por ejemplo `context.output(...)`. Puedes tambien usar entradas en conjunto,
   salidas en conjunto, accesar a la ventana, etc.

Unas pocas notas sobre como el SDK y los runners ven este DoFn:

 - Sus celdas de estado son todas declaradas explicitamente para que un SDK Beam
   o un runner puede razonar sobre ellas, por ejemplo para limpiarlas cuando una
   ventana expira.
 - Si declaras una celda de estado y entonces la usas con un tipo incorrecto, el
   SDK Java de Beam detectará ese error por tí.
 - Si declaras 2 celdas de estado con el mismo ID, el SDK también lo detectará.
 - El runner sabe que esto es un `DoFn` con estado y puede ejecutarse de manera
   muy diferente, por ejemplo para datos adicionales combinandosé y sincronizandosé
   en ordén para evitar el acceso concurrente a las celdas de estado.

Observemos en un ejemplo más de como usar esta API, está vez un poco más real.

## Ejemplo: detección de anomalías

Supongamos que estas introduciendo una serie de acciones de su usuario dentro
de algún modelo complejo para predecir alguna expresión cuantitativa del tipo de
acciones que toman, por ejemplo para detectar una actividad fraudulenta.
Construiras el modelo a partir de eventos y también comparará eventos entrantes
contra el ultimo modelo para determinar si algo ha cambiado.

Si intentas expresar la construcción de tú modelo como un `CombineFn`, puedes tener un problema con `mergeAccumulators`. Asumiendo que podrías expresar eso,
podría ser algo como esto:

```java
class ModelFromEventsFn extends CombineFn<Event, Model, Model> {
    @Override
    public abstract Model createAccumulator() {
      return Model.empty();
    }

    @Override
    public abstract Model addInput(Model accumulator, Event input) {
      return accumulator.update(input); // Esto es hecho para mutar, por eficiencia
    }

    @Override
    public abstract Model mergeAccumulators(Iterable<Model> accumulators) {
      // ?? puede escribir esto ??
    }

    @Override
    public abstract Model extractOutput(Model accumulator) {
      return accumulator; }
}
```

```py
# El estado y los temporizadores no son soportados aún dentro del SDK Python de Beam
# Mira este espacio!
```

Ahora tienes una forma para calcular el modelo de un usuario particular por una
ventana como `Combine.perKey(new ModelFromEventsFn())`. ¿Cómo aplicarias este
modelo al mismo flujo de eventos del cual es calculado? Una forma estandar para
tomar el resultado de una transformación `Combine` y usarlo mientras se hace el
procesamiento de elementos de una `PCollection` es leerlo como una entrada
paralela a una transformación `ParDo`. Por lo tanto, podrías ingresar el modelo
y revisar el flujo de eventos contra el, mostrando la predicción, de esta manera:

```java
PCollection<KV<UserId, Event>> events = ...

final PCollectionView<Map<UserId, Model>> userModels = events
    .apply(Combine.perKey(new ModelFromEventsFn()))
    .apply(View.asMap());

PCollection<KV<UserId, Prediction>> predictions = events
    .apply(ParDo.of(new DoFn<KV<UserId, Event>>() {

      @ProcessElement
      public void processElement(ProcessContext ctx) {
        UserId userId = ctx.element().getKey();
        Event event = ctx.element().getValue();

        Model model = ctx.sideinput(userModels).get(userId);

        // Perhaps some logic around when to output a new prediction
        … c.output(KV.of(userId, model.prediction(event))) … 
      }
    }));
```

```py
# El estado y los temporizadores no son soportados aún dentro del SDK Python de Beam
# Mira este espacio!
```

En esta tubería, hay solo un modelo emitido por el `Combine.perKey(...)` por
usuario, por ventana, el cual es preparado para una entrada paralela por la
transformación `View.asMap()`. El procesamiento del `ParDo` sobre eventos se
bloqueara hasta que la entrada de al lado este lista, el almacen de eventos en
búfer y entonces se revise cada evento contra el modelo. Esto es una solución
de alta latencía y alta integridad: El modelo toma en cuenta el comportamiento
del usuario en la ventana, pero no puede ser mandado a la salida hasta que la
ventana este completa.

Supongamos que deseas tener algún resultado antes o incluso no tener una ventana
natural, pero solo se quiere un analisis continuo con el "modelo hasta ahora",
aunque su modelo pueda no estar completo. ¿Puedes controlar las actualizaciones
para el modelo contra el que se estan revisando los eventos? Los disparadores son
una caracteristica generica de Beam para administrar la exhaustividad de
administración versus la compensación de latencia. Así que aquí esta la misma
tubería con un disparador agregado que manda a la salida un nuevo modelo un
segundo despues de que la entrada llega:

```java
PCollection<KV<UserId, Event>> events = ...

PCollectionView<Map<UserId, Model>> userModels = events

    // Una compensación entre latencia y costo
    .apply(Window.triggering(
        AfterProcessingTime.pastFirstElementInPane(Duration.standardSeconds(1)))

    .apply(Combine.perKey(new ModelFromEventsFn()))
    .apply(View.asMap());
```

```py
# El estado y los temporizadores no son soportados aún dentro del SDK Python de Beam
# Mira este espacio!
```

Esto es a menudo un muy buen intercambio entre latencia y costo: si una enorme
cantidad de eventos viene en un segundo, entonces solo emitiras un nuevo modelo,
por lo que no seras inundado con salidas de modelos que no puedes ni usar antes
de que se vuelvan obsoletos. En la practica, el nuevo modelo puede no estar
presente sobre el canal de entrada paralela hasta que hayan pasado muchos
segundos, debido a los retardos de detección y de procesamiento que preparan la
entrada de a un lado, muchos eventos (tal vez un lote entero de actividad) será
pasado a traves de `ParDo` y sus predicciones se calcularán de acuerdo con el
modelo anterior. Si el runner le asignó un limite suficientemente ajustado para
la expiración de la cache y usaste un disparador más agresivo, podrías ser capaz
de mejorar mejorar la latencia en costo adicional.

Pero hay otro costo que considerar: estas enviando a la salida muchos
interesantes resultados desde el `ParDo` que será procesado en sentido
descendente. Si lo "curioso" de la salida es que solo esta relativamente bien
formada con respecto de la salida aneterior, entonces no puede usar una
transformación `Filter` para reducir los datos de un volumen en sentido
descendente.

El procesamiento con estado le deja direccionear el problema de latencia de
entradas paralelas y del problema de costo de una salida no interesante y
excesiva. Aquí esta el codigo, usando solo las caracteristicas que ya he
introducido:

```java
new DoFn<KV<UserId, Event>, KV<UserId, Prediction>>() {

  @StateId("model")
  private final StateSpec<ValueState<Model>> modelSpec =
      StateSpecs.value(Model.coder());

  @StateId("previousPrediction")
  private final StateSpec<ValueState<Prediction>> previousPredictionSpec =
      StateSpecs.value(Prediction.coder());

  @ProcessElement
  public void processElement(
      ProcessContext c,
      @StateId("previousPrediction") ValueState<Prediction> previousPredictionState,
      @StateId("model") ValueState<Model> modelState) {
    UserId userId = c.element().getKey();
    Event event = c.element().getValue()

    Model model = modelState.read();
    Prediction previousPrediction = previousPredictionState.read();
    Prediction newPrediction = model.prediction(event);
    model.add(event);
    modelState.write(model);
    if (previousPrediction == null 
        || shouldOutputNewPrediction(previousPrediction, newPrediction)) {
      c.output(KV.of(userId, newPrediction));
      previousPredictionState.write(newPrediction);
    }
  }
};
```

```py
# El estado y los temporizadores no son soportados aún dentro del SDK Python de Beam
# Mira este espacio!
```

Vallamos a través de el,

 - Tienes dos celdas de estado declaradas, `@StateId("model")` para conservar el
   estado actual del modelo para un usuario y `@StateId("previousPrediction")`
   para conservar la predicción de la salida previa.
 - Accesar a las dos celdas de estado por una anotación en el método
   `@ProcessElement` es como antes.
 - Lees el modelo actual vía `modelState.read()`. Porque el estado es también
   por clave y ventana, esto es un modelo solo para el UserId del evento
   actualmente en proceso.
 - Deriva una nueva predicción `model.prediction(event) y la compara contra tú
   ultima salida, accesado vía `previousPredicationState.read()`.
 - Entonces actualizas el modelo `model.update()` y lo escribes vía
   `modelState.write(...)`. Esta perfectamente bien mutar el valor que sacas de
   cada estado siempre que también recuerdes escribir el valor mutado, en la
   misma forma que se recomienda mutar los acumuladores `CombineFn`.
 - Si la predicción ha cambiado una cantidad significativa desde la última vez
   que la envio a la salida, la emitaras via`context.output(...)` y guardas la
   predicción usando `previousPredictionState.write(...)`. Aqui la desición es
   relativa a la predicción de salida anterior, no a la última calculada
   es posible que tenga aqui algunas condiciones complejas.

¡La mayoria de lo anterior es solo una charla a través de Java! Pero antes de que
vaya y convierta todas sus tuberías para que usen procesamiento con estado,
quiero comentar algunas consideraciones sobre si es una buena consideración para
su caso de uso.

## Consideraciones de rendimiento

Para decidir si usar el estado por-clave-y-ventana, necesitas considerar como se
ejecuta. Puedes profundizar sobre como un runner en particular administra el
estado, pero hay algunas cosas generales que debe tener en mente:

 - EL particionamiento por clave y ventana: quizas lo más importante a considerar
   es que el runner puede tener que mezclar tus datos para colocar todos los datos
   para una particular clave+ventana. Si los datos ya estan mezclados
   correctamente, el runner puede tomar ventaja de esto.
 - Gastos generales de sincronización: la API esta diseñada para que el runner
   se encargue del control de concurrencia, pero esto significa que el runner no
   puede hacer procesamiento paralelo de los elementos para un particular
   clave+ventana incluso cuando de lo contrario sería ventajoso.
 - El almacenamiento y tolerancia a fallos del estado: desde que el estado es
   por-clave-y-ventana, cuantas más claves y ventanas esperes procesar
   simultaneamente, mayor será el almacenamiento en el que incurriras. Porque el
   estado beneficia de toda la tolerancia a fallos/propiedades de consistencia
   de tus otros datos en Beam, también suma el costo de confirmar los resultados
   de procesamiento.
 - Expiración de estado: también desde que el estado es por-ventana, el runner
   puede reclamar los recursos cuando una ventana expira (cuando un marcador
   excede su retraso permitido) pero esto podría significar que el runner esta
   rastreando un temporizador adicional por clave y ventana para causar la
   recuperación de codigo a ejecutar.

## ¡Ve y usalo!

Si eres nuevo en Beam, espero que ahora estes interesado en ver si Beam con
procesamiento con estado aborda tu caso de uso. Si ya tienes experiencia en
Beam, espero que esta nueva adición al modelo de Beam desbloque nuevos casos
de uso para tí. Revisa la [matriz de disponibildad de capacidades]({{ site.baseurl }}/documentation/runners/capability-matrix/) para
ver el nivel de soporte para esta nueva caracteristica del modelo en tu o
tus backends favoritos.

Y por favor unase a la comunidad en
[user@beam.apache.org]({{ site.baseurl }}/get-started/support). Nos encantaría
saber de usted.

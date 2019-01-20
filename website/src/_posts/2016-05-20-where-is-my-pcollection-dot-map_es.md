---
layout: post
title:  "¿Dondé esta mi PCollection.map()?"
date:   2016-05-27 09:00:00 -0700
excerpt_separator: <!--more-->
categories: blog
authors:
  - robertwb
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
¿Alguna vez te has preguntado porqué Beam tiene PTransforms para todo en lugar de tener métodos para ello en PCollection? Vamos a echar un vistazo a la historia que llevo a estas (y otras) decisiones de diseño.

<!--more-->

Aunque Beam es relativamente nuevo, su diseño se basa en muchos años de experiencia con tuberías (pipelines) del mundo real. Una de las inspiraciones es [FlumeJava](https://ai.google/research/pubs/pub35650), el cual es el sucesor interno de Google para MapReduce introducido por primera vez en 2009.

La API original de FlumeJava tiene métodos como `count` y `parallelDo` en las PCollections. Aunque está ligeramente expresado de manera más conciso y preciso, esté enfoque tiene muchas desventajas de extensibilidad. Cada nuevo usuario de FlumeJava deseo agregar transformaciones, y agregarlas como métodos a un PCollection simplemente no escala bien. En cambio, un PCollection en Beam tiene un solo método `apply` que toma cualquier PTransform como un argumento.

<table class="table">
  <tr>
    <th>FlumeJava</th>
    <th>Beam</th>
  </tr>
  <tr>
    <td><pre>
PCollection&lt;T&gt; input = …
PCollection&lt;O&gt; output = input.count()
                             .parallelDo(...);
    </pre></td>
    <td><pre>
PCollection&lt;T&gt; input = …
PCollection&lt;O&gt; output = input.apply(Count.perElement())
                             .apply(ParDo.of(...));
    </pre></td>
  </tr>
</table>

Esté es un enfoque más escalable por varias razones.

## ¿Dónde trazar la línea?
La adición de métodos a PCollection fuerza a una linea a ser trazada entre operaciones que son suficientemente "útiles" para merecer esté tratamiento especial y esos que no lo son. Es fácil hacerlo con flat map, agrupar por clave, y combinar por clave. ¿Pero qué pasa con filter? ¿Con Count? ¿Aproximación de conteo? ¿Aproximación de cuantiles? ¿El más frecuente? ¿EscribirAMiFuenteFavorita? Al ir más lejos en este camino llegamos a una unica clase enorme que contiene casi todo lo que alguien quisiera hacer (La clase PCollection de FlumeJava tiene más de 5000 lineas con alrededor de 70 operaciones distintas, y pudo haber sido más grande si hubiéremos aceptado cada propuesta). Además, debido a que Java no permite agregar métodos a una clase, hay una fuerte división sintáctica entre las operaciones que son agregadas a PCollection y aquellas que no son agregadas. Una forma tradicional de compartir código es con una biblioteca de funciones, pero las funciones (al menos en lenguajes tradicionales como Java) se escriben con un estilo de prefijo, que no se combina bien con el estilo del generador de flujos (por ejemplo: `input.operation1().operation2().operation3()` vs. `operation3(operation1(input).operation2())`).

En cambio, en Beam hemos elegido un estilo que coloca todas las transformaciones--si son operaciones primitivas, operaciones compuestas agrupadas en el SDK o parte de una biblioteca externa--en igualdad de condiciones. Esto también facilita implementaciones alternativas (las cuales pueden incluso tomar diferentes opciones) que son fácilmente intercambiables.

<table class="table">
  <tr>
    <th>FlumeJava</th>
    <th>Beam</th>
  </tr>
  <tr>
    <td><pre>
PCollection&lt;O&gt; output =
    ExternalLibrary.doStuff(
        MyLibrary.transform(input, myArgs)
            .parallelDo(...),
        externalLibArgs);
    </pre></td>
    <td><pre>
PCollection&lt;O&gt; output = input
    .apply(MyLibrary.transform(myArgs))
    .apply(ParDo.of(...))
    .apply(ExternalLibrary.doStuff(externalLibArgs));
    &nbsp;
    </pre></td>
  </tr>
</table>

## Configurabilidad
Hace que un estilo fluido permita que los valores (PCollections) sean los objetos pasados y manipulados (esto es, los manejadores para el gráfico de ejecución diferida), pero son las operaciones mismas las que deben ser compilables, configurables y ampliables. El uso de los métodos de PCollection para las operaciones no escalan bien aquí, especialmente en un lenguaje sin argumentos por default o argumentos de palabras clave. Por ejemplo, una operación ParDo puede tener cualquier número de entradas y salidas adicionales, o una operación de escritura puede tener configuraciones relacionadas con la codificación y la compresión. Una opción es separarlos en múltiples sobrecargas o incluso métodos, pero eso exacerba los problemas anteriores (¡FlumeJava evolucionó a lo largo de una docena de sobrecargas del método `parallelDo`!). Otra opción es pasar a cada método un objeto de configuración que se puede construir utilizando expresiones idiomáticas más fluidas como el patrón de construcción, pero en ese punto uno podría hacer al objeto de configuración la operación en sí misma, que es lo que hace Beam.

## Seguridad de Tipos
Muchas operaciones solo pueden ser aplicadas a colecciones cuyos elementos son de un tipo específico. Por ejemplo, la operación GroupByKey solo debe aplicarse a `PCollection<KV<K, V>>`. Al menos en Java, no es posible restringir los métodos basándose únicamente en el tipo de elemento recibido como parámetro. En FlumeJava, esto nos llevó a agregar un `PTable<K, V>` que es subclase de `PCollection <KV <K, V >>` para contener todas las operaciones específicas de PCollections en pares clave-valor. Esto conduce a la misma pregunta sobre qué tipos de elementos son lo suficientemente especiales como para ser capturados por las subclases de PCollection. Eso no es muy extensible para terceras partes y a menudo, requiere casts/conversiones manuales (que no se pueden encadenar de forma segura en Java) y operaciones especiales que producen estas especializaciones de PCollection.

Esto es particularmente inconveniente para las transformaciones que producen salidas cuyos tipos de elemento son iguales a (o derivados de) los tipos de elementos de sus entradas, requieren soporte extra para generar la subclase correcta (por ejemplo, un filter en una PTable debería producir otra PTable en vez de solo una PCollection cruda con pares clave-valor).

Usar PTransforms nos permite evitar todo este problema. Podemos colocar restricciones arbitrarias en el contexto en el que se puede usar una transformación en función del tipo de sus entradas; por ejemplo GroupByKey es estaticamentente tipado para solo ser aplicado a `PCollection<KV<K, V>>`. La forma en que esto sucede es generalizable a formas arbitrarias, sin necesidad de introducir tipos especializados como PTable.

## Reusabilidad y Estructura
Aunque los PTransforms generalmente se construyen en el sitio en que son usados, al extraerlos como objetos separados, uno es capaz de almacenarlos y pasarlos.

A medida que las tuberías crecen y evolucionan, es útil estructurar tu tubería en componentes modulares, a menudo reutilizables, y las PTransforms permiten hacer esto muy bien en una tubería de procesamiento de datos. En adición, las PTransforms modulares también exponen la estructura lógica de tu código al sistema (por ejemplo, para monitoreo). De las tres diferentes representaciones de la tubería del WordCount de abajo, solo la vista estructurada captura la intención de alto nivel de la tubería. El hacer que incluso las operaciones más simples sean PTransforms permite que sea más sencillo empaquetar varias operaciones de PTransforms compuestas.

<img class="center-block" src="{{ "/images/blog/simple-wordcount-pipeline.png" | prepend: site.baseurl }}" alt="Tres Visualizaciones diferentes de una simple tubería del WordCount" width="500">

<div class="text-center">
<i>Tres Visualizaciones diferentes de una simple tubería del WordCount que calcula el numero de ocurrencias de cada palabra en un conjunto de archivos de texto. La vista plana proporciona el DAG completo de todas las operaciones realizadas. La vista de ejecución agrupa operaciones de acuerdo a como se ejecutan, por ejemplo, después de realizar optimizaciones especificas de ejecución como la composición de la función. La vista estructurada anida operaciones de acuerdo a su agrupación en PTransforms.</i>
</div>

## Resumen
Aunque es tentador agregar métodos a PCollections, no es un enfoque escalable, extensible, suficientemente expresivo. Poner un solo método en PCollection y toda la lógica dentro de la operación misma nos deja tener lo mejor de ambos mundos, y evita los acantilados de complejidad al tener un estilo único y consistente a través de tuberías simples y complejas, y entre operaciones predefinidas y definidas por el usuario.

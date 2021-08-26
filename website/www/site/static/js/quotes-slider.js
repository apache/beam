// Licensed under the Apache License, Version 2.0 (the 'License'); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an 'AS IS' BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

var slider = new KeenSlider("#my-keen-slider", {
  loop: true,
  created: function (instance) {
    var dots_wrapper = document.getElementById("dots");
    var slides = document.querySelectorAll(".keen-slider__slide");
    slides.forEach(function (t, idx) {
      var dot = document.createElement("button");
      dot.classList.add("dot");
      dots_wrapper.appendChild(dot);
      dot.addEventListener("click", function () {
        instance.moveToSlide(idx);
      });
    });
    updateClasses(instance);
  },
  slideChanged(instance) {
    updateClasses(instance);
  }
});

function updateClasses(instance) {
  var slide = instance.details().relativeSlide;

  var dots = document.querySelectorAll(".dot");
  dots.forEach(function (dot, idx) {
    idx === slide
      ? dot.classList.add("dot--active")
      : dot.classList.remove("dot--active");
  });
}
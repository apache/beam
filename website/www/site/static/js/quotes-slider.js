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
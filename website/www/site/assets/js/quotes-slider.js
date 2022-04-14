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

$(document).ready(function(){
  const SelectorNames = {
    slider: 'slick-JS',
    dotsWrapper: 'dots-wrapper',
    dots: 'dots',
  }

  $(`.${SelectorNames.slider}`).slick({
    mobileFirst: true,
    infinite: true,
    slidesToShow: 1,
    slidesToScroll: 1,

    arrows: false,
    dots: true,
    appendDots: `.${SelectorNames.dotsWrapper}`,
    dotsClass: SelectorNames.dots,
    responsive: [
      {
        breakpoint: 1024,
        settings: {
          infinite: false,
          slidesToShow: 3,
          slidesToScroll: 3,
        }
      },
    ]
  });
});

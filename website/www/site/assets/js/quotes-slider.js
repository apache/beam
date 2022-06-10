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

new Swiper('.swiper', {
  loop: false,
  slidesPerView: 1,
  slidesPerGroup: 1,
  pagination: {
    el: ".swiper-pagination",
    clickable: true,
    bulletClass: "bullet-class-custom",
    bulletActiveClass: "bullet-active-class-custom",
  },
  navigation: {
    nextEl: ".swiper-button-next-custom",
    prevEl: ".swiper-button-prev-custom",
  },
  breakpoints: {
    // switches from mobile to desktop when window width is > 1024px
    1025: {
      slidesPerView: 3,
      slidesPerGroup: 3,
    },
  }
});

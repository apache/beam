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

$(document).ready(function() {
  function PageNav(conf) {
    var idPageNav = conf["classNamePageNav"];
    var idMainContainer = conf["classNameMainContainer"];

    var CONST = {
      DESKTOP_BREAKPOINT: 1024,
      PAGENAV_WIDTH: 248
    };

    function toggleChosen(sideNavItem){
      $(sideNavItem).toggleClass("chosen")
    };

    function showCollapse(sideNavItem){
      $('ul', sideNavItem).first().collapse('show');
    };

    function toggleRotate(img){
      $(img).toggleClass('rotate');
    }

    function removeRotate(img){
      $(img).removeClass('rotate');
    }

    function setNavEvents(){
      var sideNavItems = $(".page-nav > #TableOfContents li");
      //Collapse all nav items
      $('ul', sideNavItems).addClass("collapse");

      //Toggles collapsible nav items when clicked and already selected
      $('a:first', sideNavItems).click(function(){
        var isChosen = $(this).parent('li.chosen').length;

        if(isChosen){
          $(this).siblings('ul').collapse('toggle');
          toggleRotate($('img', this));
        }
      });

      //Checks every time a nav item is activated by the scroll-spy
      //Doesn't trigger if the same element is clicked again
      $('[data-spy="scroll"]').on('activate.bs.scrollspy', function (e) {
        var lastActive = $(".page-nav > #TableOfContents li.active").last()
        var lastChosen = $(".page-nav > #TableOfContents li.chosen")
        var isChosen = $(e.target).hasClass('chosen').length

        toggleChosen(lastChosen)
        toggleChosen(lastActive);

        showCollapse(e.target);

        if(!(isChosen)){
          var img = $('img', e.target).first();
          removeRotate(img)
        }
      });

    }

    return {
      "idPageNav": idPageNav,
      "idMainContainer": idMainContainer,
      "setNavEvents": setNavEvents,

      "setPageNav": function() {
        var mainContainerData = {
          width: $("." + idMainContainer).width(),
          offset: $("." + idMainContainer).offset()
        };

        if(window.innerWidth > CONST.DESKTOP_BREAKPOINT) {
          $("." + idPageNav).css({
            left: mainContainerData.offset.left +  mainContainerData.width - CONST.PAGENAV_WIDTH
          });
        } else {
          $("." + idPageNav).css({
            left: 0
          });
        }
      },

      "bindEvents": function() {
        var _self = this;

        $(window).resize(function() {
          _self.setPageNav();
        });
      },

      "prependArrows": function () {
          var items = $(".page-nav > #TableOfContents li");
          var itemTags = $('ul', items).siblings('a');
          var img = document.createElement("img");
          img.src = "{{ "images/arrow-expandable.svg" | absURL }}";
          img.classList="rotate";

          $(itemTags).prepend(img);
      },

      "refreshScrollSpy": function() {
        $('[data-spy="scroll"]').each(function () {
          $(this).scrollspy('refresh');
        });
      },

      "isContainerPresent": function () {
        return $("." + idMainContainer).length > 0
      },

      "init": function() {
        if (this.isContainerPresent()) {
          this.bindEvents();
          this.setPageNav();
          this.prependArrows();
          this.setNavEvents();
          this.refreshScrollSpy();
        }
      }
    }
  }

  PageNav(
    {
      "classNamePageNav":"page-nav",
      "classNameMainContainer": "container-main-content"
    }
  ).init();
});

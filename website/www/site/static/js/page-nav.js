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
    function checkList () {
          var items = $(".page-nav > #TableOfContents li");
          if (items && items.length > 0) {
            for(var i=0; i<items.length; i++){
              if(items[i].classList.contains('chosen')){
                items[i].classList.remove('chosen');
              };
              if(items[i].classList.contains('active') || items[i].classList[0] == 'active'){
                if($('> a',items[i]).length>0){
                  if(!items[i].querySelector('a').getAttribute('aria-expanded') || items[i].querySelector('a').getAttribute('aria-expanded') == 'false'){
                      items[i].classList.add('chosen');
                    }
                  }
                }
              }
              if($('#TableOfContents .active').length === 0 || $('#TableOfContents .chosen').length===0){
                if($('#TableOfContents li').first().children('a').length === 0){
                  $('#TableOfContents li').first().children('ul').children('li')[0].classList.add('chosen');
                  $('#TableOfContents li')[0].classList.add('active');
                }
                else
                  $('#TableOfContents li')[0].classList.add('chosen');
              }
            }
    };
    return {
      "idPageNav": idPageNav,
      "idMainContainer": idMainContainer,

      "setPageNav": function() {
        var mainContainerData = {
          width: $("." + idMainContainer).width(),
          offset: $("." + idMainContainer).offset()
        };

        if($(window).width() > CONST.DESKTOP_BREAKPOINT) {
          $("." + idPageNav).css({
            left: mainContainerData.offset.left +  mainContainerData.width - CONST.PAGENAV_WIDTH
          });
        } else {
          $("." + idPageNav).css({
            left: 0
          });
        }
        if($('#TableOfContents .active').length === 0){
          if($('#TableOfContents li').first().children('a').length === 0){
            $('#TableOfContents li').first().children('ul').children('li')[0].classList.add('chosen');
            $('#TableOfContents li')[0].classList.add('active');
          }
          else
            $('#TableOfContents li')[0].classList.add('chosen');
        }
      },

      "bindEvents": function() {
        var _self = this;

        $(window).resize(function() {
          _self.setPageNav();
        });
      },
      "setActiveItemClassEvent": function () {
        $("." + idSectionNav + " a").click(function (e) {
            var currentItem = document.querySelector(classNavActiveItem);
            if (currentItem)
                currentItem.classList.remove(CONST.ACTIVE_CLASS);
            e.target.classList.add(CONST.ACTIVE_CLASS);
        });
    },

    "displayActiveItem": function () {
        document.querySelector(".page-nav > #TableOfContents").addEventListener('click', function(){
          setTimeout(checkList,50)
        });
        document.addEventListener('scroll', checkList);
        var items = $(".page-nav > #TableOfContents li");
        var img = document.createElement("img");
        img.src = "/images/arrow-expandable.svg";
        img.classList="rotate";
        for(i=0; i<items.length; i++){
          if(items[i].querySelector('ul')){
            if(items[i].querySelector(':first-child').tagName != "UL"){
              items[i].querySelector('a').dataset.target ="#collapse"+i;
              items[i].querySelector('a').dataset.toggle ="collapse";
              items[i].querySelector('ul').classList.add('collapse');
              items[i].querySelector('ul').id = "collapse"+i;
              items[i].querySelector('a').insertAdjacentElement('afterbegin',img);
              items[i].querySelector('a').addEventListener('click', function(e){
                this.querySelector('img').classList.toggle('rotate');
                e.preventDefault();
          })
          }}
        }
    },

      "init": function() {
        this.bindEvents();
        this.setPageNav();
        this.displayActiveItem();
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

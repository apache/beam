$(document).ready(function() {
  function PageNav(conf) {
    var idPageNav = conf["classNamePageNav"];
    var idMainContainer = conf["classNameMainContainer"];

    var CONST = {
      DESKTOP_BREAKPOINT: 1024,
      PAGENAV_WIDTH: 240
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
      },

      "bindEvents": function() {
        var _self = this;

        $(window).resize(function() {
          _self.setPageNav();
        });
      },

      "init": function() {
        this.bindEvents();
        this.setPageNav();
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

$(document).ready(function() {
  function Navbar(conf) {
    var idCTA = conf["classNameCTA"],
        idContainer = conf["classNameContainer"],
        idNavMask = conf["classNameMask"],
        idBackCTA = conf["classNameBackCTA"],
        idSectionNav = conf["classNameSectionNav"];

    var CONST = {
      OPEN_CLASS: "open",
      DESKTOP_BREAKPOINT: 1024
    };

    return {
      "idCTA": idCTA,
      "idContainer": idContainer,
      "idNavMask": idNavMask,
      "idSectionNav": idSectionNav,
      "idBackCTA": idBackCTA,
      "hasSectionNav": false,

      "bindEvents": function() {
          var _self = this;
          var sectionNavEl = $("." + idSectionNav);
          var sectionNavHeight = $(sectionNavEl).height();

          $(".container-main-content").css({"min-height": sectionNavHeight});

          $(window).resize(function() {
            if($(window).width() > CONST.DESKTOP_BREAKPOINT) {
              var sectionNavHeight = $(sectionNavEl).height();
              $(".container-main-content").css({"min-height": sectionNavHeight});
            }else {
              $(".container-main-content").css({"min-height": ''});
            }
          });

          if(_self.hasSectionNav) {
            $("." + _self.idCTA ).click(function(el) {
              $("." + _self.idNavMask).addClass(CONST.OPEN_CLASS);
              $("." + _self.idSectionNav).addClass(CONST.OPEN_CLASS);
            });

            $("." + _self.idBackCTA).click(function(el) {
              $("." + _self.idSectionNav).removeClass(CONST.OPEN_CLASS);
              $("." + _self.idContainer).addClass(CONST.OPEN_CLASS);
            });
          } else {
            $("." + _self.idCTA ).click(function(el) {
              $("." + _self.idNavMask).addClass(CONST.OPEN_CLASS);
              $("." + _self.idContainer).addClass(CONST.OPEN_CLASS);
            });
          }

          $("." + _self.idNavMask ).click(function(el) {
            $("." + _self.idNavMask).removeClass(CONST.OPEN_CLASS);
            $("." + _self.idContainer).removeClass(CONST.OPEN_CLASS);

            if(_self.hasSectionNav) {
              $("." + _self.idSectionNav).removeClass(CONST.OPEN_CLASS);
            }
          });
      },
      "findSectionNav": function() {
        var sectionNavEl = $('body').find("[data-section-nav]");

        if(sectionNavEl.length) {
          this.hasSectionNav = true;
        }
      },
      "init": function() {
        this.findSectionNav();
        this.bindEvents();
      }
    }
  }

  Navbar(
    {
      "classNameContainer":"navbar-container",
      "classNameSectionNav": "section-nav",
      "classNameBackCTA": "section-nav-back",
      "classNameCTA": "navbar-toggle",
      "classNameMask": "navbar-mask"
    }
  ).init();
});

$(document).ready(function() {
  function FixMenu(conf) {
    var idMenu = conf["classNameMenu"];
    var CONST = {
      DESKTOP_BREAKPOINT: 1024
    };

    return {
      "idMenu": idMenu,
      "menuConfig": {},

      "bindEvents": function() {
        var _self = this;

        $(window).scroll(function(e) {
          if($(window).width() > CONST.DESKTOP_BREAKPOINT) {
            var scroll = $(window).scrollTop();
            var scrollMenu = scroll + $("." + idMenu).height() + _self.menuConfig.offsetTop;
            var scrollHeight = scroll + $(window).height();
            var bodyHeightOffset = $('body')[0].scrollHeight - _self.menuConfig.offsetBottom;

            if (scroll <= _self.menuConfig.offsetTop) {
              $("." + idMenu).css({
                top: '',
                bottom: ''
              });
            } else if (scroll > _self.menuConfig.offsetTop && scrollMenu < bodyHeightOffset) {
              $("." + idMenu).css({
                top: _self.menuConfig.offsetTop,
                bottom: ''
              });
            } else if (scrollMenu >= bodyHeightOffset) {
              $("." + idMenu).css({
                top: '',
                bottom: scrollHeight - bodyHeightOffset
              });
            }
          } else {
            $("." + idMenu).css({
              top: '',
              bottom: ''
            });
          }
        });

        $(window).resize(function(e) {
          if($(window).width() <= CONST.DESKTOP_BREAKPOINT) {
            $("." + idMenu).css({
              top: '',
              bottom: ''
            });
          }
        });
      },
      "init": function() {
        this.menuConfig = $("." + this.idMenu).data();
        this.bindEvents();
      }
    }
  }

  FixMenu({"classNameMenu":"section-nav"}).init();
  FixMenu({ "classNameMenu":"page-nav" }).init();
});

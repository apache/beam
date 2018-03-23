$(document).ready(function() {
    function Navbar(conf) {
        var idCTA = conf["classNameCTA"],
            idContainer = conf["classNameContainer"],
            idNavMask = conf["classNameMask"],
            idBackCTA = conf["classNameBackCTA"],
            idSectionNav = conf["classNameSectionNav"],
            idNavItemCollapsible = conf["classNameNavItemCollapsible"];

        var CONST = {
            OPEN_CLASS: "open",
            DESKTOP_BREAKPOINT: 1024,
            EXPANDED_CLASS: "expanded"
        };

        return {
            "idCTA": idCTA,
            "idContainer": idContainer,
            "idNavMask": idNavMask,
            "idSectionNav": idSectionNav,
            "idBackCTA": idBackCTA,
            "hasSectionNav": false,

            "setCollapsibleBehaviourItems" : function () {
                $("." + idNavItemCollapsible).click(function(e) {
                    var item = $(e.target),
                        sectionNav = item.parent('li'),
                        expanded = sectionNav.hasClass(CONST.EXPANDED_CLASS),
                        sectionNavList = item.next('ul');

                    if(expanded){
                        sectionNavList.slideUp().fadeOut(600,function(){});
                        sectionNav.removeClass(CONST.EXPANDED_CLASS);
                    }else{
                        sectionNavList.slideDown().fadeIn(600,function(){ });
                        sectionNav.addClass(CONST.EXPANDED_CLASS);
                    }

                    e.stopPropagation();
                });
            },
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

                this.setCollapsibleBehaviourItems();
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
            "classNameMask": "navbar-mask",
            "classNameNavItemCollapsible" : "section-nav-item--collapsible span"
        }
    ).init();
});

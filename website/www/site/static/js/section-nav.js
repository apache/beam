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

$(document).ready(function () {
    function Navbar(conf) {
        var idCTA = conf["classNameCTA"],
            idContainer = conf["classNameContainer"],
            idNavMask = conf["classNameMask"],
            idBackCTA = conf["classNameBackCTA"],
            idSectionNav = conf["classNameSectionNav"],
            idNavItemTitleCollapsible = conf["classNameNavItemTitleCollapsible"],
            classNavItemCollapsible = conf["classNameNavItemCollapsible"],
            classNavActiveItem = conf["classNameNavActiveItem"];

        var CONST = {
            ACTIVE_CLASS: "active",
            EXPANDED_CLASS: "expanded",
            DESKTOP_BREAKPOINT: 1024,
            OPEN_CLASS: "open"
        };

        var expandCollapseItem = function (item, effect) {
            var sectionNav = item.parent('li'),
                expanded = sectionNav.hasClass(CONST.EXPANDED_CLASS),
                sectionNavList = item.next('ul');

            if (expanded) {
                if (effect) {
                    sectionNavList.slideUp().fadeOut(600);
                } else {
                    sectionNavList.hide();
                }
                sectionNav.removeClass(CONST.EXPANDED_CLASS);
            } else {
                if (effect) {
                    sectionNavList.slideDown().fadeIn(600);
                } else {
                    sectionNavList.show();
                }

                sectionNav.addClass(CONST.EXPANDED_CLASS);
            }
        };


        return {
            "idCTA": idCTA,
            "idContainer": idContainer,
            "idNavMask": idNavMask,
            "idSectionNav": idSectionNav,
            "idBackCTA": idBackCTA,
            "hasSectionNav": false,

            "setCollapsibleBehaviourItems": function () {
                $("." + idNavItemTitleCollapsible).click(function (e) {
                    var item = $(e.target);
                    expandCollapseItem(item, true);
                    e.stopPropagation();
                });
            },

            "addClassToTableOfContents": function () {
                $("#TableOfContents").children().addClass("nav");
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
                const currentLocation = window.location.pathname + window.location.hash;
                var activeItem = document.querySelectorAll("nav [href='" + currentLocation + "']");
                if (activeItem && activeItem.length > 0) {
                    activeItem = activeItem[0];
                    activeItem.classList.add(CONST.ACTIVE_CLASS);
                    var collapsibleParents = $(activeItem).parents('li' + classNavItemCollapsible);
                    for (var i = 0; collapsibleParents.length > i; i++) {
                        var item = $(collapsibleParents[i]).find('span')[0];
                        expandCollapseItem($(item), false);
                    }
                }
            },

            "bindEvents": function () {
                var _self = this;
                var sectionNavEl = $("." + idSectionNav);
                var sectionNavHeight = $(sectionNavEl).height();
                var mainContent = $(".container-main-content");

                mainContent.css({"min-height": sectionNavHeight});
                sectionNavEl.css({"max-height": mainContent.css("height")});

                $(window).resize(function () {
                    if ($(window).width() > CONST.DESKTOP_BREAKPOINT) {
                        var sectionNavHeight = $(sectionNavEl).height();
                        $(".container-main-content").css({"min-height": sectionNavHeight});
                    } else {
                        $(".container-main-content").css({"min-height": ''});
                    }
                });

                if (_self.hasSectionNav) {
                    $("." + _self.idCTA).click(function (el) {
                        $("." + _self.idNavMask).addClass(CONST.OPEN_CLASS);
                        $("." + _self.idSectionNav).addClass(CONST.OPEN_CLASS);
                    });

                    $("." + _self.idBackCTA).click(function (el) {
                        $("." + _self.idSectionNav).removeClass(CONST.OPEN_CLASS);
                        $("." + _self.idContainer).addClass(CONST.OPEN_CLASS);
                    });
                } else {
                    $("." + _self.idCTA).click(function (el) {
                        $("." + _self.idNavMask).addClass(CONST.OPEN_CLASS);
                        $("." + _self.idContainer).addClass(CONST.OPEN_CLASS);
                    });
                }

                $("." + _self.idNavMask).click(function (el) {
                    $("." + _self.idNavMask).removeClass(CONST.OPEN_CLASS);
                    $("." + _self.idContainer).removeClass(CONST.OPEN_CLASS);

                    if (_self.hasSectionNav) {
                        $("." + _self.idSectionNav).removeClass(CONST.OPEN_CLASS);
                    }
                });

                this.addClassToTableOfContents();
                this.setCollapsibleBehaviourItems();
                this.setActiveItemClassEvent();
                setTimeout(function () {
                    this.displayActiveItem();
                }.bind(this), 0);

            },
            "findSectionNav": function () {
                var sectionNavEl = $('body').find("[data-section-nav]");

                if (sectionNavEl.length) {
                    this.hasSectionNav = true;
                }
            },
            "init": function () {
                this.findSectionNav();
                this.bindEvents();
            }
        }
    }

    Navbar(
        {
            "classNameContainer": "navbar-container",
            "classNameSectionNav": "section-nav",
            "classNameBackCTA": "section-nav-back",
            "classNameCTA": "navbar-toggle",
            "classNameMask": "navbar-mask",
            "classNameNavItemTitleCollapsible": "section-nav-item--collapsible span",
            "classNameNavItemCollapsible": ".section-nav-item--collapsible",
            "classNameNavActiveItem": ".section-nav a.active"
        }
    ).init();
});

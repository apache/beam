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
    function Switcher(conf) {
        var id = conf["class-prefix"],
            def = conf["default"];
        var prefix = id + "-";
        return {
            "id": id,
            "selector": "[class^=" + prefix + "]:not(.no-toggle)",
            "wrapper": prefix + "switcher", // Parent wrapper-class.
            "default": prefix + def, // Default type to display.
            "dbKey": id, // Local Storage Key

            /**
             * @desc Generate bootstrapped like nav template,
                    showing supported types in nav.
             * @param array $types - list of supported types.
             * @return string - html template, which is bootstrapped nav tabs.
            */
            "navHtml": function(types) {
                var lists = "";
                var selectors = "";

                types.forEach(function(type) {
                    var name = type.replace(prefix, "");
                    name = (name === "py")? "python": name;
                    name = name.charAt(0).toUpperCase() + name.slice(1);
                    selectors += " " + type;
                    lists += "<li data-type=\"" + type + "\"><a>";
                    lists += name + "</a></li>";
                });
                return "<div class=\"" + this.wrapper + selectors + "\"> \
                        <ul class=\"nav nav-tabs\">" + lists + "</ul> </div>";
            },
            /**
             * @desc Extract language from provided text.
             * @param string $text - string containing language, e.g language-python.
             * @return string - cleaned name of language, e.g python.
             */
            "parseName": function(str) {
                var re = new RegExp(prefix + "(\\w+)");
                var parse = re.exec(str);
                return (parse) ? parse[1] : "";
            },
            /**
             * @desc Add Navigation tabs on top of parent code blocks.
             */
            "addTabs": function() {
                var _self = this;

                $("div"+_self.selector).each(function() {
                    if ($(this).prev().is("div"+_self.selector)) {
                        return;
                    }
                    $(this).before(_self.navHtml(_self.lookup($(this), [])));
                });
            },
            /**
             * @desc Search next sibling and if it's also a code block, then store
                    its type and move on to the next element. It will keep
                    looking until there is no direct code block descendant left.
             * @param object $el - jQuery object, from where to start searching.
             * @param array $lang - list to hold types, found while searching.
             * @return array - list of types found.
            */
            "lookup": function(el, lang) {
                if (!el.is("div"+this.selector)) {
                    return lang;
                }

                lang.push(el.attr("class").split(" ")[0])
                return this.lookup(el.next(), lang)
            },
            "bindEvents": function() {
                var _self = this;
                $("." + _self.wrapper + " ul li").click(function(el) {
                    // Making type preferences presistance, for user.
                    localStorage.setItem(_self.dbKey, $(this).data("type"));
                    _self.toggle();
                });
            },
            "toggle": function() {
                var pref=localStorage.getItem(this.dbKey) || this.default;
                var isPrefSelected = false;

                // Adjusting active elements in navigation header.
                $("." + this.wrapper + " li").removeClass("active").each(function() {
                    if ($(this).data("type") === pref) {
                        $(this).addClass("active");
                        isPrefSelected = true;
                    }
                });

                if(!isPrefSelected) {
                  pref = this.default;

                  $("." + this.wrapper + " li").each(function() {
                      if ($(this).data("type") === pref) {
                          $(this).addClass("active");
                      }
                  });
               }

                // Swapping visibility of code blocks.
                $(this.selector).hide();
                $("nav"+this.selector).show();
                $("." + pref).show();
            },
            "render": function(wrapper) {
                this.addTabs();
                this.bindEvents();
                this.toggle();
            }
        };
    }

    Switcher({"class-prefix":"language","default":"java"}).render();
    Switcher({"class-prefix":"runner","default":"direct"}).render();
    Switcher({"class-prefix":"shell","default":"unix"}).render();
    Switcher({"class-prefix":"version"}).render();
});

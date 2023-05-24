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
    function getElementData(event) {
        const clickedLangSwitchEl = event.target;
        const elPreviousOffsetFromViewPort = clickedLangSwitchEl.getBoundingClientRect().top;
        return {
            elPreviousOffsetFromViewPort,
            clickedLangSwitchEl,
        }
    }

    function setScrollToNewPosition(clickedElementData) {
        const { elPreviousOffsetFromViewPort, clickedLangSwitchEl } = clickedElementData;
        const elCurrentHeight = window.pageYOffset + clickedLangSwitchEl.getBoundingClientRect().top;
        $('html, body').scrollTop(elCurrentHeight - elPreviousOffsetFromViewPort);
    }

    const loadTime = new Date();

    function Switcher(conf) {
        const name = conf["name"];

        return {
            ...conf,
            "uniqueValues": new Set(),
            "selector": `[class^=${name}-]:not(.no-toggle)`, // Ex: [class^=language-]:not(.no-toggle)
            "wrapper": `${name}-switcher`, // Parent wrapper-class.
            "localStorageKey": name,

            /**
             * @desc Generate bootstrapped like nav template,
                    showing supported types in nav.
             * @param array $types - list of supported types.
             * @return string - html template, which is bootstrapped nav tabs.
            */
            "navHtml": function (values) {
                let lists = "";
                let classes = "";

                for (const value of values) {
                    const title = this.valueToTabTitle(value);
                    classes += ` ${name}-${value}`;
                    lists += `<li class="langSwitch-content" data-value="${value}">${title}</li>`;
                }

                // Ex: language-switcher language-java language-py language-go
                return `<div class="${this.wrapper + classes}"><ul class="nav nav-tabs">${lists}</ul></div>`;
            },

            "valueToTabTitle": function (value) {
                switch (value) {
                    case 'py': return 'Python';
                    case 'scio': return 'SCIO';
                    case 'typescript': return 'TypeScript';
                }

                return value.charAt(0).toUpperCase() + value.slice(1);
            },

            /**
             * @desc Add Navigation tabs on top of parent code blocks.
             */
            "addTabs": function() {
                var _self = this;

                // Iterate over all content blocks to insert tabs before.
                // If multiple content blocks go in a row, only insert tabs before the first one.
                $("div" + _self.selector).each(function() { // Ex: div[class^=language-]:not(.no-toggle)
                    if ($(this).prev().is("div" + _self.selector)) {
                        return; // Not the first one.
                    }

                    const values = _self.findNextSiblingsValues($(this));
                    for (const value of values) {
                        _self.uniqueValues.add(value);
                    }

                    const tabsHtml = _self.navHtml(values);
                    $(this).before(tabsHtml);

                    // Add 'language-XXX' classes to the Playground iframe so it can be hidden
                    // when selecting a language it does not have.
                    let iframeWrapper = $(this).parent().parent().children('.code-snippet-playground');
                    if (iframeWrapper.length > 0) {
                        for (const value of values) {
                            iframeWrapper.addClass(`${name}-${value}`)
                        }
                    }
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
            "findNextSiblingsValues": function(el, lang) {
                if (!el.is("div" + this.selector)) {
                    return [];
                }

                const prefix = `${name}-`;
                for (const cls of el[0].classList) {
                    if (cls.startsWith(prefix)) {
                        return [
                            cls.replace(prefix, ""),
                            ...this.findNextSiblingsValues(el.next()),
                        ];
                    }
                }

                return [];
            },

            "bindEvents": function() {
                var _self = this;
                $(`.${_self.wrapper} li`).click(function(event) {
                    localStorage.setItem(_self.localStorageKey, $(this).data("value"));

                    // Set scroll to new position because Safari and Firefox
                    // can't do it automatically, only Chrome under the hood
                    // detects the correct position of viewport
                    const clickedLangSwitchData = getElementData(event);
                    _self.toggle(false);
                    setScrollToNewPosition(clickedLangSwitchData);
                });
            },

            "toggle": function(isInitial) {
                let value = localStorage.getItem(this.localStorageKey) || this.default;
                let hasTabForValue = $(`.${this.wrapper} li[data-value="${value}"]`).length > 0; // Ex: .language-switcher li[data-value="java"]

                if (!hasTabForValue) {
                    // if there's a code block for the default language,
                    // set the preferred language to the default language
                    if (this.uniqueValues.has(this.default)) {
                        value = this.default;
                    } else {
                        // otherwise set the preferred language to the first available
                        // language, so we don't have a page with no code blocks
                        value = [...this.uniqueValues][0];
                    }
                }

                $(`.${this.wrapper} li[data-value="${value}"]`).addClass("active");
                $(`.${this.wrapper} li[data-value!="${value}"]`).removeClass("active");

                // Hiding all sets of tabs and all playground iframes.
                // Then showing those having a tab for the new language.
                $(this.selector).hide(); // Ex: [class^=language-]:not(.no-toggle)
                if (name === 'language') {
                    $('.code-snippet-playground').hide();
                }
                $(`.${name}-${value}`).show();

                // Showing the main switch at the top of the page.
                $("nav" + this.selector).show();

                // make sure that runner and shell snippets are still visible after changing language
                $("code" + this.selector).show();

                //add refresh method because html elements are added/deleted after changing language
                $('[data-spy="scroll"]').each(function () {
                    $(this).scrollspy('refresh');
                });

                if (this.onChanged) {
                    this.onChanged(value, isInitial);
                }
            },
            "render": function(wrapper) {
                this.addTabs();
                this.bindEvents();
                this.toggle(true);
            },
        };
    }

    Switcher({
        "name": "language",
        "default": "java",

        "onChanged": function (lang, isInitial) {
            if (isInitial) {
                this.onInit(lang);
            } else {
                this.onChangedAfterLoaded(lang);
            }
        },

        /**
         * @desc Called after language is determined for the first time.
         *       Modifies the iframes' URLs by adding the language so we don't need to
         *       send messages to them. This is cheap when the page only started loading.
         */
        "onInit": function (lang) {
            const playgroundIframeContainers = document.getElementsByClassName("code-snippet-playground");
            const sdk = this.langToSdk(lang);

            for (const div of playgroundIframeContainers) {
                const src = div.dataset.src;
                const width = div.dataset.width;
                const height = div.dataset.height;

                const url = new URL(src);
                const searchParams = new URLSearchParams(url.search);
                searchParams.set("sdk", sdk);
                url.search = searchParams.toString();

                const iframe = document.createElement('iframe');
                iframe.src = url.href;
                iframe.width = width;
                iframe.height = height;
                iframe.className = 'playground';
                iframe.allow = 'clipboard-write';

                div.appendChild(iframe);
            }
        },

        /**
         * @desc Called when the user switched the language tab manually.
         */
        "onChangedAfterLoaded": function (lang) {
            const playgroundIframes = $(".code-snippet-playground iframe").get();
            const message = {
                type: "SetSdk",
                sdk: this.langToSdk(lang),
            };

            const _self = this;
            let attempts = 30;

            // If another cycle of sending these messages is running, stop it.
            clearInterval(this.interval);

            const sendMessage = function () {
                for (const iframe of playgroundIframes) {
                    iframe.contentWindow.postMessage(message, '*');
                }

                if (attempts-- === 0) {
                    clearInterval(_self.interval);
                }
            };

            if (!this.areFramesLoaded()) {
                // The guess is that the iframes may not have loaded yet.
                // If we just send a message, Flutter may have not yet set its listener.
                // So send the message with intervals in hope some of them are received.
                // Playground ignores duplicate messages in a row.
                this.interval = setInterval(sendMessage, 1000);
            }

            sendMessage();
        },

        "langToSdk": function (lang) {
            switch (lang) {
                case "py": return "python";
            }
            return lang;
        },

        /**
         * @desc A rough guess if the embedded iframes are loaded, timer-based.
         *       Experiments hint that ~10 seconds is sufficient on slowest devices.
         */
        "areFramesLoaded": function () {
            const millisecondsAgo = new Date() - loadTime;
            return millisecondsAgo >= 30000;
        },
    }).render();

    Switcher({"name": "runner", "default": "direct"}).render();
    Switcher({"name": "shell", "default": "unix"}).render();
    Switcher({"name": "version"}).render();
});

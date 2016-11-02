$(document).ready(function(){
    // Pattern in every code block, wrapper element.
    var elAttr = "div[class^=\"language-\"]";
    // Default language.
    var defLang = "language-java";
    // Wrapper class for css stylings.
    var wrapper="language-switcher"

    /**
      * @desc Generate bootstrapped like nav template, showing supported languages in nav.
      * @param array $languages - list of supported languages.
      * @return string - html template, which is bootstrapped nav tabs.
    */
    function navTemplate(languages) {
        var langList="";

        for (var i = languages.length - 1; i >= 0; i--) {
            langList+="<li data-lang=\""+languages[i]+"\"> \
                <a>"+languages[i].replace("language-","")+"</a></li>";
        }
        return "<div class=\""+wrapper+" "+languages.join(" ")+"\"> \
                <ul class=\"nav nav-tabs\">"+langList+"</ul> </div>";
    }

    /**
      * @desc Extract language from provided text.
      * @param string $text - string containing language, e.g language-python.
      * @return string - cleaned name of languge, e.g python.
    */
    function getLang(text) {
        var lang = /language-\w+/i.exec(text)
        return (lang)? lang[0]:"";
    }

    /**
      * @desc Search next sibling and if it's also a code block, then store
            it's language and move onto the next element. It will keep
            looking untill their is no direct code block decendent left.
      * @param object $el - jQuery object, from where to start searching.
      * @param array $lang - list to hold languages, found while searching.
      * @return array - list of languages found.
    */
    function nextFetch(el, lang) {
        if(!el.is(elAttr)) {
            return lang;
        }

        lang.push(getLang(el.attr("class")))
        return nextFetch(el.next(), lang)
    }

    /**
      * @desc Perform language switch on page, and persist it's state.
    */
    var switchLanguage = function() {
        var langPref = localStorage.getItem("codePreference") || defLang;

        // Adjusting active elements in navigation header.
        $("."+wrapper+" li").removeClass("active").each(function(){
            if($(this).data("lang") === langPref) {
                $(this).addClass("active");
            }
        });

        // Swapping visibility of code blocks.
        $("[class^=\"language-\"]").hide();
        $("."+langPref).show();
    }

    // Add Nav tabs on top of code blocks.
    $(elAttr).each(function() {
        if($(this).prev().is(elAttr)) {
            return;
        }
        $(this).before(navTemplate(nextFetch($(this), [])));
    });

    // Attaching on click listener, to li elements.
    $("."+wrapper+" ul li").click(function(el) {
        // Making language preferences presistance, for user.
        localStorage.setItem("codePreference", $(this).data("lang"));
        switchLanguage();
    })

    // Invoking on page boot.
    switchLanguage();
})
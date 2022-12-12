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
    function copy() {
        $(".copy").click(function(){
            var text=$(this).siblings()[$(this).siblings().length-1].childNodes[0].innerText;
            const el=document.createElement('textarea');
            el.value=text;document.body.appendChild(el);
            el.select();document.execCommand('copy');
            document.body.removeChild(el);
        })
        $(".just-copy").click(function(){
            var text=$(this).parent().siblings()[0].innerText;
            const el=document.createElement('textarea');
            el.value=text;document.body.appendChild(el);
            el.select();document.execCommand('copy');
            document.body.removeChild(el);
        })
    }
    let code = document.querySelectorAll('pre'),
    copyIcon = document.createElement('span');
    copyIcon.innerHTML = '<a class="just-copy" type="button" data-bs-toggle="tooltip" data-bs-placement="bottom" title="Copy to clipboard"><img src="{{ "images/copy-icon.svg" | absURL }}"/></a>';

    code.forEach((hl) => {
        if( !hl.parentElement.classList.contains('code-snippet') && !hl.parentElement.classList.contains('highlight')) {
            const textNode = hl.innerHTML;
            hl.innerHTML = `<div class="pre-content-container">${textNode}</div>`
            hl.prepend(copyIcon.cloneNode([true]));
        }
    })
   copy();
});

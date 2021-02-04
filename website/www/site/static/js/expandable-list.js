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
    function expandable(){
        function stop(){
            var w = $(window).width();
            $('.arrow-list-header').click(function(e){
                if (w <= 768) {
                    e.preventDefault();
                    e.stopPropagation();
                }
            })
        }
    }
    $(window).resize(function(){
        stop();
    });
    function rotate() {
        $(".arrow-list-header").click(function(){
            $(this).find('figure').toggleClass('rotate');
        });
        $('#apache-dropdown').click(function(){
            $(this).find('span').toggleClass('rotate');
        });
    }
   rotate();
   expandable();
});

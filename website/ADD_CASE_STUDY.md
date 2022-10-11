<!--
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# How to add new case study card

1. Fork [Apache Beam](https://github.com/apache/beam) repository
2. Copy a [case study draft template](https://docs.google.com/document/d/1qRpXW-WM4jtlcy5VaqDaXgYap9KI1ii27Uwp641UOBM/edit#heading=h.l6lphj20eacs) that provides some helpful tips, questions and ideas to organize your case study.
3. Copy file [case study md template](https://github.com/apache/beam/tree/master/website/CASE_STUDY_TEMPLATE.md)
4. Rename file with case study or project name to the case-studies folder e.g., /beam/website/www/site/content/en/case-studies/YOUR_CASE_STUDY_NAME.md
5. Add Images to the image folder beam/website/www/site/static/images according to [images instructions](#adding-images-recommendations)
6. Change the template according to your description. See [case study instructions](#filling-out-new-case-study-recommendations)
7. Add case study quote card to the main page. See [main page case study instructions](#filling-out-mainPage-case-study-card-recommendations)
8. Create pull request to the apache beam repository with your changes

## Adding Images Recommendations

- Add project/case study logo to the beam/website/www/site/static/images/logos/powered-by/ folder. Please use your case study/project name e.g. YOUR_CASE_STUDY.png
- Add author image to the beam/website/www/site/static/images/authorImage.png
- If you need to add a picture that doesn't suite in any of above-mentioned cases, add it to the folder beam/website/www/site/static/images/

## Filling Out New Case Study Recommendations
- `title`: "Case study title" <!-- required property, short case study title, usually from 4 to 12 words --!>
- `name`: "Case study Name" <!-- required property, short case study title, usually from 5-6 to 12 words --!>
- `icon`: `/images/logos/powered-by/templateIcon.png`
- `category`: study <!-- optional property, for case-studies use study, for `also used by` just skip it--!>
- `cardTitle`: "Lorem Ipsum is simply dummy text of the printing and typesetting industry." <!-- optional property, usually from 5-6 same as title, for `also used by` just skip it--!>
- `cardDescription`: "Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, when an unknown printer took a galley of type and scrambled it to make a type specimen book." <!-- required property, usually from 30 to 40 words --!>
- `authorName`: "Name LastName" <!-- optional property, for `also used by` just skip it--!>
- `authorPosition`: "Software Engineer @ companyName" <!-- optional property, for `also used by` just skip it--!>
- `authorImg`: `/images/authorImage.png` <!-- optional property, for `also used by` just skip it--!>
- `publishDate`: 2022-10-14T01:56:00+00:00 <!-- optional property, for `also used by` just skip it--!>

Above-mentioned properties determines how your case-study looks on page with all case-studies `https://beam.apache.org/case-studies/`. You also need to fill in the information for a separate page like `https://beam.apache.org/case-studies/lyft/`. Generally a case-study page consists of multiple text sections, 1-2 blocks with images, couple of quotes. All sections should be titled. Exemplary structure of the card is given in [case study md template](https://github.com/apache/beam/blob/master/website/CASE_STUDY_TEMPLATE.md). 

## Filling Out MainPage Case Study Card Recommendations
- add new case study quote card to (quotes.yaml)[https://github.com/apache/beam/blob/master/website/www/site/data/en/quotes.yaml]
- use template below
```
  text: Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since the 1500s. // recomendation to use no more than 215 symbols in the text
  icon: icons/quote-icon.svg
  logoUrl: images/logos/powered-by/YOUR_CASE_STUDY_NAME.svg
  linkUrl: case-studies/YOUR_CASE_STUDY_NAME/index.html
  linkText: Learn more
```

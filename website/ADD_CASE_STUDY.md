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

# How to add a new case study

1. Fork [Apache Beam](https://github.com/apache/beam) repository
2. This [case study draft template](https://docs.google.com/document/d/1qRpXW-WM4jtlcy5VaqDaXgYap9KI1ii27Uwp641UOBM/edit#heading=h.l6lphj20eacs) provides some helpful tips, questions and ideas to prepare and organize your case study content
3. Copy [case study md template](https://github.com/apache/beam/tree/master/website/CASE_STUDY_TEMPLATE.md) to the `case-studies` folder and name your file with company or project name  e.g., `beam/website/www/site/content/en/case-studies/YOUR_CASE_STUDY_NAME.md`
4. Add your case study content to the md file you just created. See [Case study md file recommendations](#case-study-md-file-recommendations)
5. Add images to the image folder [beam/website/www/site/static/images/case-study](https://github.com/apache/beam/tree/master/website/www/site/static/images/case-study)/company-name according to [Case study images recommendations](#case-study-images-recommendations)
6. Add case study quote card for the [Apache Beam](https://beam.apache.org/) website homepage `Case Studies Powered by Apache Beam` section. See [Add case study card to the Apache Beam website homepage](#Add-case-study-card-to-the-Apache-Beam-website-homepage)
7. Create pull request to the apache beam repository with your changes

If you have any questions about adding a case study, please send an email to dev@beam.apache.org with subject: [Beam Website] Add New Case Study.

## Case study md file recommendations

Following properties determine how your case-study will looks on [Apache Beam case studies](https://beam.apache.org/case-studies/) listing and the case study page itself.

| Field                 | Description                                                                                                                    |
|-----------------------|--------------------------------------------------------------------------------------------------------------------------------|
| `title`               | Case study title, usually 4-12 words                                                                                           |
| `name`                | Company or project name                                                                                                        |
| `icon`                | Relative path to the company/project logo e.g. "/images/logos/powered-by/company_name.png"                                     |
| `category`            | `study` for case studies                                                                                                       |
| `cardTitle`           | Case study card title for Apache Beam [case studies](https://beam.apache.org/case-studies/) page                               |
| `cardDescription`     | Description for [case studies](https://beam.apache.org/case-studies/) page, usually 30-40 words                                |
| `authorName`          | Case study author                                                                                                              |
| `coauthorName`        | Case study additional author, optional param                                                                                   |
| `authorPosition`      | Case study author role                                                                                                         |
| `coauthorPosition`    | Case study additional author role, optional param                                                                              |
| `authorImg`           | Relative path for case study author photo, e.g. "/images/case-study/company/authorImg.png"                                     |
| `coauthorImg`         | Relative path for case study second author photo, e.g. "/images/case-study/company/authorImg.png", optional param              |
| `publishDate`         | Case study publish date for sorting at [case studies](https://beam.apache.org/case-studies/), e.g. `2022-10-14T01:56:00+00:00` |

Other sections of the [case study md template](https://github.com/apache/beam/blob/master/website/CASE_STUDY_TEMPLATE.md) are organized to present the case study content.

## Case study images recommendations

1. Add case study company/project logo to the [images/logos/powered-by](https://github.com/apache/beam/tree/master/website/www/site/static/images/logos/powered-by) folder. Please use your company/project name e.g. `ricardo.png`
2. Create your company/project folder to group images used in your case study e.g., `beam/website/www/site/static/images/case-study/company-name` folder
3. Add author photo to `beam/website/www/site/static/images/case-study/company-name` folder
4. Add other images that your case study is using to `beam/website/www/site/static/images/case-study/company-name` folder


## Add case study card to the Apache Beam website homepage

To add a new case study card to the Apache Beam website homepage, add the new case study entry to the [quotes.yaml](https://github.com/apache/beam/blob/master/website/www/site/data/en/quotes.yaml) using the following format:

| Field             | Description                                                                                             |
|-------------------|---------------------------------------------------------------------------------------------------------|
| `text`            | Homepage case study text, recommended up to 215 characters or so                                        |
| `icon`            | Relative path to quotation marks logo, by default `icons/quote-icon.svg`                                |
| `logoUrl`         | Relative path for company/project logo, e.g. `images/logos/powered-by/company_name.png`                 |
| `linkUrl`         | Relative path to the case study web page, e.g., `case-studies/YOUR_CASE_STUDY_NAME/index.html`          |
| `linkText`        | Link text, by default using `Learn more`                                                                |

Example:
```
  text: Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since the 1500s. // recommendation to use no more than 215 symbols in the text
  icon: icons/quote-icon.svg
  logoUrl: images/logos/powered-by/company_name.png
  linkUrl: case-studies/YOUR_CASE_STUDY_NAME/index.html
  linkText: Learn more
```

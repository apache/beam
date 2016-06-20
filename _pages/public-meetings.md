---
layout: page
title: "Apache Beam Public Meetings"
permalink: /public-meetings/
---
Apache Beam is a shared effort within the open source community. To grow and develop that effort, it helps to schedule and hold public meetings, including:

* Formal meetings for the Apache Beam community for project planning and discussion
* Informal "Meetup" events to discuss, share the vision and usefulness of Apache Beam

The goal of these meetings will vary, though they will typically focus on technical discussions, community matters, and decision-making. These meetings have been held or are scheduled by the Apache Beam community.

<table class="table">
  <tr>
      <th>Date & Time</th>
      <th>Location</th>
      <th>Type</th>
      <th>Meeting materials</th>
      <th>Notes</th>
  </tr>
  {% for meeting in site.data.meetings.events %}
    <tr>
      <td>{{ meeting.date }}<br>{{ meeting.time }}</td>
      <td>{{ meeting.location }}</td>
      <td>{{ meeting.type }}</td>
      <td>
        {% for material in meeting.materials %}
        <div><a href="{{ material.link }}">{{ material.title }}</a></div>
        {% endfor %}
      </td>
      <td>{{ meeting.notes }}</td>
    </tr>
  {% endfor %}
</table>
*This list was last updated on {{ site.data.meetings.last_updated }}.*

All Apache Beam meetings are open to the public and we encourage anyone to attend. From time to time space in our event location may be limited, so preference will be given to PPMC members and others on a first-come, first-serve basis.

## I want to give a public talk about Apache Beam
To get started, we recommend you review the Apache Beam [presentation materials]({{ site.baseurl }}/presentation-materials/) page to review the content the Apache Beam community has already created. These materials will possibly save you time and energy as you create content for your event.

Once you have scheduled your event, we welcome you to announce it on the [user@beam.incubator.apache.org](mailto:user@beam.incubator.apache.org) mailing list. Additionally, please open a [JIRA item](https://issues.apache.org/jira/browse/BEAM) using the `website` component with details so we can update this page.

If you have any questions as you prepare for your event, we recommend you reach out to the Apache Beam community through the [dev@beam.incubator.apache.org](mailto:dev@beam.incubator.apache.org) mailing list. The Beam community can help provide feedback on your materials and promote your event.

## Frequently asked questions about public meetings

### How can I remotely attend these meetings?
We are investigating methods, such as video conferencing and IRC chat, to allow anyone to attend remotely. At present we do not have an estimated date. If you are interested in this option, please send an email to the [user@beam.incubator.apache.org](mailto:user@beam.incubator.apache.org) with your ideas and recommendations.

### What is a public meeting?
Public meetings include scheduled Apache Beam Dev/PPMC meetings, Meetup events, conference talks, and other events where the public meets to discuss Beam.

### How do I learn about new meetings?
The Apache Beam community announces upcoming public meetings on the  [dev@beam.incubator.apache.org](mailto:dev@beam.incubator.apache.org) mailing list. If you want to learn about new events, we recommend you [subscribe](mailto:dev-subscribe@beam.incubator.apache.org) to that list.  If you are holding a public event, please send an email to the dev@ list.

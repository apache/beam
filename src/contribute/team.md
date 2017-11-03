---
layout: section
title: "Beam Team"
section_menu: section-menu/contribute.html
permalink: /contribute/team/
redirect_from:
  - /project/team/
  - /team/
---

# Apache Beam Team

A successful project requires many people to play many roles. Some members write code or documentation, while others are valuable as testers, submitting patches and suggestions.

The team is comprised of Members and Contributors. Members have direct access to the source of a project and actively evolve the code-base. Contributors improve the project through submission of patches and suggestions to the Members. The number of Contributors to the project is unbounded. Get involved today. All contributions to the project are greatly appreciated.

{% for team in site.beam_team %}
  <h2>{{ team.group }}</h2>
  <p>{{ team.description }}</p>
  <table class="table table-hover">
    <thead>
      <tr>
        <th>Name</th>
        <th>Apache ID</th>
        <th>Email</th>
        <th>Organization</th>
        <th>Roles</th>
        <th>Time Zone</th>
      </tr>
    </thead>
    <tbody>
      {% for member in team.members %}
        <tr>
          <th scope="row">{{ member.name }}</th>
          <td scope="row">{{ member.apache_id }}</td>
          <td scope="row">{{ member.email }}</td>
          <td scope="row">{{ member.organization }}</td>
          <td scope="row">{{ member.roles }}</td>
          <td scope="row">{{ member.time_zone }}</td>
        </tr>
      {% endfor %}
    </tbody>
  </table>
{% endfor %}

import os
import re
import typing
import yaml

from jinja2 import Environment, FileSystemLoader
from typing import Dict, Union, List, Any
from apache_beam.transforms.temp_wrapper_provider import ExternalSchemaTransformProvider
from apache_beam.transforms.temp_wrapper_provider import STANDARD_URN_PATTERN
from apache_beam.transforms.external import BeamJarExpansionService


SUPPORTED_SDK_DESTINATIONS = ['python']

def generate_schematransform_config(input_services, output_schematransforms_file):
  transform_list: Dict[str, Dict[str, Any]] = {}
  with open(input_services) as f:
    services = yaml.safe_load(f)
    for service in services:
      target = service['gradle_target']
      provider = ExternalSchemaTransformProvider(BeamJarExpansionService(target))
      service_destinations = service['destinations']
      for sdk, destination in service_destinations.items():
        if sdk not in SUPPORTED_SDK_DESTINATIONS:
          raise ValueError(f"Service with target {target} specifies a "
                           f"destination for an invalid SDK: {sdk}. The "
                           f"supported SDKs are {SUPPORTED_SDK_DESTINATIONS}")
      for urn, wrapper in provider.get_all().items():
        if 'transforms' in service and urn in service['transforms']:
          modified_transform = service['transforms'][urn]
        else:
          modified_transform = {}
        # use modified name if specified, else use the inferred wrapper name
        if 'name' in modified_transform:
          name = modified_transform['name']
        else:
          name = wrapper.__name__
        # We infer the destination from the URN and service destination.
        # For example, the Java IO expansion service defaults to Python
        # package apache_beam.io. Kafka Write is a transform in this service
        # with URN beam:schematransform:org.apache.beam:kafka_write:v1
        # In this case, we infer the destination apache_beam.io.kafka_write
        functionality_identifier = re.match(STANDARD_URN_PATTERN, urn).groups()[0]
        destinations = {sdk: f"{destination}.{functionality_identifier}" for sdk, destination in service_destinations.items()}
        # update with any modified destinations
        if 'destinations' in modified_transform:
          for sdk, destination in modified_transform['destinations'].items():
            if sdk not in SUPPORTED_SDK_DESTINATIONS:
              raise ValueError(f"Identifier {urn} specifies a destination for "
                               f"an invalid SDK: [{sdk}]. The supported SDKs "
                               f"are {SUPPORTED_SDK_DESTINATIONS}")
            destinations[sdk] = destination

        fields = {}
        for param in wrapper.configuration_schema.values():
          tp = param.type
          nullable = False
          if typing.get_origin(tp) is Union and type(None) in typing.get_args(tp):
            nullable = True
            # unwrap and set type to the original
            args = typing.get_args(tp)
            if len(args) == 2:
              tp = args[0]

          if tp.__module__ == 'builtins':
            tp = tp.__name__
          elif tp.__module__ == 'typing':
            tp = str(param.type).replace("typing.", "")
          elif tp.__module__ == 'numpy':
            tp = "%s.%s" % (tp.__module__, tp.__name__)
          field_info = {'type': tp,
                        'description': param.description,
                        'nullable': nullable}
          fields[param.original_name] = field_info

        transform = {'identifier': urn, 'name': name, 'destinations': destinations,
                     'default_service': target, 'fields': fields,
                     'description': wrapper.description}
        transform_list[urn] = transform

  with open(output_schematransforms_file, 'w') as f:
    yaml.dump(list(transform_list.values()), f)

from apache_beam.transforms.temp_wrapper_provider import camel_case_to_snake_case

def parse_schematransform_config(config_file):
  env = Environment(loader=FileSystemLoader("./"))
  template = env.get_template("wrapper_template.template")

  # maintain a list of wrappers to write in each file. if custom destinations
  # are used, we may end up with multiple wrappers in one file.
  destinations: Dict[str, List[str]] = {}

  with open(config_file) as f:
    schematransforms = yaml.safe_load(f)
    for st in schematransforms:
      default_service = st['default_service']
      description = st['description']
      destination = st['destinations']['python']
      name = st['name']
      fields = st['fields']
      urn = st['identifier']

      parameters = []
      for param, info in fields.items():
        pythonic_name = camel_case_to_snake_case(param)
        param_details = {"name": pythonic_name,
                         "type": info['type'],
                         "description": info['description'],
                         "original_name": param}
        if info['nullable']:
          param_details["default"] = None

        parameters.append(param_details)
      parameters = sorted(parameters, key=lambda p: 'default' in p)

      default_service = f"{BeamJarExpansionService.__name__}(\"{default_service}\")"
      render = template.render(class_name=name, identifier=urn, parameters=parameters,
                               description=description,
                               default_expansion_service=default_service)
      print(render)
      if destination not in destinations:
        destinations[destination] = []
      destinations[destination].append(render)

  for dest, wrappers in destinations.items():
    dest += "_stw.py"
    with open(dest, "w") as file:
      file.write("# <Apache license>\n")
      file.write("# NOTE: This file is autogenerated and should not be edited by hand.\n")
      file.write(
        "from apache_beam.transforms.external import BeamJarExpansionService\n"
        "from apache_beam.transforms.temp_wrapper_provider import ExternalSchemaTransform\n")
      for wrapper in wrappers:
        file.write("\n\n")
        file.write(wrapper + "\n")


if __name__ == '__main__':
  input = os.path.join(os.path.dirname(__file__), 'standard_expansion_services.yaml')
  output = os.path.join(os.path.dirname(__file__), 'dumped.yaml')

  # args: clean? generate?

  # if len(typing.get_args(tp)) == 2:
  #
  # tp = typing.get_args(tp)
  # generate_schematransform_config(
  #   input_services=input,
  #   output_schematransforms_file=output)

  parse_schematransform_config(output)
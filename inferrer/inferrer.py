import logging
from argparse import ArgumentParser
import os.path
import json

import schema_serializer
import inferrer

logging.basicConfig(level=logging.INFO)

if __name__ == '__main__':
    argp = ArgumentParser(description="Schema inferrer")
    argp.add_argument("--file-path", "-f", required=True, dest='file_path',
                      help="Data file whose schema is to be interred")
    argp.add_argument("--file_type", "-t", required=True, dest='file_type', help="File type")
    argp.add_argument("--output-path", "-o", required=True, dest='output_path', help="Output path")

    args = argp.parse_args()
    file = args.file_path
    type = args.file_type
    basedir = args.output_path

    logging.info("Inferring schema %s", args)
    #TODO: move this loop to the inferrer module
    n_objs = 100
    n_max = 5000
    while n_objs < n_max:
        try :
            # Infer schema
            schema, properties = inferrer.from_local(file, type, n_objs=n_objs)
            logging.info("Schema: %s; Properties: %s" % (schema, properties))
            serialized_schema = schema_serializer.serialize(schema)
            break
        except schema_serializer.SerializerException as e:
            logging.warn("Exception: %s" % e)
            logging.info('Could not infer type with %d, retrying with %d' % (n_objs, 2*n_objs))
            n_objs = 2*n_objs

    # basedir = os.path.dirname(file)
    serialized_schema = schema_serializer.serialize(schema)


    logging.debug("Serialized Schema:\n%s" % serialized_schema)
    schemaFile = os.path.join(basedir, "schema.xml")
    logging.info("Writing schema: " + schemaFile)
    with open(schemaFile, "w") as text_file:
        text_file.write(serialized_schema)

    serialized_properties = json.dumps(properties)
    propFile = os.path.join(basedir, "properties.json")
    logging.info("Writing properties file: " + propFile)
    with open(propFile, "w") as text_file:
        text_file.write(serialized_properties)

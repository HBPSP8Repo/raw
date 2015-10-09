import logging
from argparse import ArgumentParser
import os.path
import json
import os
import shutil

import schema_serializer
import inferrer

scala_data = os.environ['SCALA_DATA']

logging.basicConfig(level=logging.INFO)

if __name__ == '__main__':
    argp = ArgumentParser(description="Schema inferrer")
    argp.add_argument("--file-path", "-f", required=True, dest='file_path',
                      help="Data file whose schema is to be interred")
    argp.add_argument("--file_type", "-t", required=True, dest='file_type', help="File type")
    argp.add_argument("--schema-name", "-n", required=True, dest='schema_name', help="Schema name")
    argp.add_argument("--user", "-u", required=False, dest='user', help="User Name")
    argp.add_argument('-F', '--force', help='Will delete schema-path before registering', action='store_true')


    args = argp.parse_args()
    file = args.file_path
    type = args.file_type
    name = args.schema_name
    user = args.user

    if not args.user:
        print 'ERROR: user not defined'
        print 'Available options'
        users = os.listdir(scala_data)
        for u in users:
            print '\t', u
        sys.exit(1)
    
    # will put everything directly in the $SCALA_DATA 
    basedir = os.path.join(scala_data, user, name)
    if os.path.exists(basedir):
        if not args.force:
            raise Exception("Schema name already registered")
        else:
            shutil.rmtree(basedir)

    os.makedirs(basedir)
    # creates a symlink of the file 
    link= os.path.join(basedir, os.path.basename(file))
    os.symlink(file, link)
    logging.info("Inferring schema %s", args)
    # Infer schema
    schema, properties = inferrer.from_local(name, file, type)
    logging.info("Schema: %s; Properties: %s" % (schema, properties))

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

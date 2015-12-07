#!/usr/bin/env python
import logging
from argparse import ArgumentParser
import os.path
import json
import os
import shutil
import sys 

import schema_serializer
import inferrer

scala_data = os.environ['SCALA_DATA']

logging.basicConfig(level=logging.INFO)

def register_file(path, user, force=False):
    # extracts name and type from the filename 
    basename = os.path.basename(path)
    parts = os.path.splitext(basename)
    name = parts[0]    
    extension = parts[1].lower()
    #just removes the dot in the beginning 
    file_type = extension[1:] if extension[0] == '.' else extension
    if file_type not in inferrer.supported_types():
        logging.warn("not registering unknon file type "+ path)
        return

    basedir = os.path.join(scala_data, user, name)        
     # will put everything directly in the $SCALA_DATA 
    if os.path.exists(basedir):
        if not force:
            raise Exception("Schema name already registered")
        else:
            shutil.rmtree(basedir)
    os.makedirs(basedir)

    try:
        # creates a symlink of the file 
        link= os.path.join(basedir, "%s.%s" %(name,file_type))
        os.symlink(path, link)
                   
        n_objs = 100
        n_max = 1000
        while n_objs < n_max:
            try :
                # Infer schema
                schema, properties = inferrer.from_local(path, file_type, n_objs=n_objs)
                logging.info("Schema: %s; Properties: %s" % (schema, properties))
                serialized_schema = schema_serializer.serialize(schema)
                break
            except schema_serializer.SerializerException :
                logging.info('Could not infer type with %d, retrying with %d' % (n_objs, 2*n_objs))
                n_objs = 2*n_objs
                
        logging.info("Serialized Schema:\n%s" % serialized_schema)
        schemaFile = os.path.join(basedir, "schema.xml")
        logging.debug("Writing schema: " + schemaFile)
        with open(schemaFile, "w") as text_file:
            text_file.write(serialized_schema)

        serialized_properties = json.dumps(properties)
        propFile = os.path.join(basedir, "properties.json")
        logging.debug("Writing properties file: " + propFile)
        with open(propFile, "w") as text_file:
            text_file.write(serialized_properties)
        logging.info("registered file %s, name=%s, type=%s" % (path, name, file_type  ))
    except Exception as e:
        # if something fails deletes the folder
        logging.error("could not register %s, deleting %s" % (path, basedir))
        shutil.rmtree(basedir)
        raise
        
if __name__ == '__main__':
    argp = ArgumentParser(description="Schema inferrer")
    argp.add_argument("--file-path", "-f", required=True, dest='file_path',
                      help="Data file whose schema is to be interred")
    argp.add_argument("--user", "-u", required=False, dest='user', help="User Name")
    argp.add_argument('-F', '--force', help='Will delete schema-path before registering', action='store_true')
    argp.add_argument('-i', '--ignore', help='will ignore errors', action='store_true')

    args = argp.parse_args()
    path = args.file_path
    user = args.user
    
    logging.debug("Inferring schema %s", args)
    
    if not args.user:
        print 'ERROR: user not defined'
        print 'Available options'
        users = os.listdir(scala_data)
        for u in users:
            print '\t', u
        sys.exit(1)
       
    if os.path.isdir(path):
        for dirpath, dirnames, filenames in os.walk(path):
            for f in filenames:
                try:
                    filename = os.path.join(dirpath, f)
                    register_file( filename , user, force=args.force)
                except Exception as e:
                    if not args.ignore:
                        raise
                    else:
                        logging.error("Could not register fie %s: %s " %(filename, e))
    else:
         register_file( path, user, force= args.force)    

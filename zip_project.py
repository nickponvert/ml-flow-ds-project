'''Simple utility script to zip the project sources into a zip file.
Useful for docker images which don't have zip binaries.
'''
import os
import zipfile

def zipdir(path, ziph):
    # ziph is zipfile handle
    for root, dirs, files in os.walk(path):
        for file in files:
            ziph.write(os.path.join(root, file))

if __name__ == '__main__':
    zipf = zipfile.ZipFile('project.zip', 'w', zipfile.ZIP_DEFLATED)
    zipdir('project/', zipf)
    zipf.close()
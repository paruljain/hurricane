from os import scandir

def scanDir(path):
    global filesScanned
    if shutdown:
        return
    try:
        for file in scandir(path):
            if shutdown:
                return
            fullPath = join(path, file.name)
            if file.is_file():
                fileQ.put(fullPath, True)
                filesScanned += 1
            elif file.is_dir():
                scanDir(fullPath)
    except:
        pass # Ignore folder access permission errors
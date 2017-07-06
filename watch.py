from os import listdir
from os.path import isfile
import sys
import cPickle as pickle
import sys
import argparse

sys.path.append('/hgsccl_software/prod/')
from flufl.lock import Lock


RECORD_DIR = "/hgsccl/directoryWatcherRecords"

class DirectoryWatcher:
    """
    Compare the contents of a directory against a pickled "file record".
    If the record has changed, update the record and print the files that are new.

    """

    def __init__(self, watchDir):
        print watchDir
        self.watchDir = watchDir
        self.fileRecordName = watchDir.lstrip("/").replace("/", ".")

    def getRecordPath(self):
        return "%s/%s" % (RECORD_DIR, self.fileRecordName)

    # TODO split check and update
    def check(self):
        # list the files in the watchDir
        files = set()
        for f in listdir(self.watchDir):
            if isfile("%s/%s" % (self.watchDir, f)):
                files.add(f)

        lockFile = "%s.%s" % (self.getRecordPath(), "lock")
        # load the file record
        # if the file record doesnt exist, create an empty list
        if not isfile(self.getRecordPath()):
            # TODO could wrap with a lock
            pickle.dump(files, open(self.getRecordPath(), "w"))

        storedFiles = pickle.load(open(self.getRecordPath(), "r"))
            
        newFiles = []
        # check if there are any new files in the file record
        # if there are, add them to the record
        for f in files:
            if f not in storedFiles:
                newFiles.append(f)        
        return newFiles

    def update(self):
        # TODO wrap with lock
        newFiles = self.check()

        storedFiles = pickle.load(open(self.getRecordPath(), "r"))
        for f in newFiles:
            storedFiles.add(f)

        # save the new record
        if len(newFiles) > 0:
            pickle.dump(storedFiles, open(self.getRecordPath(), "w"))

        return newFiles

def test():
    dw = DirectoryWatcher(RECORD_DIR +  "/testing")

    print dw.check()

    print dw.update()

    print dw.check()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='''Check for new files or Update and list the changed files''', formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("command", help="check or update")
    parser.add_argument("watchDir", help="The directory to monitory for files")
    args = parser.parse_args()

    dw = DirectoryWatcher(args.watchDir)
    if args.command == "check":
        print dw.check()
    elif args.command == "update":
        print dw.update()
    else:
        assert False

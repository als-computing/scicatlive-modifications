import h5py
import json
import sys
import datetime
import hashlib
import urllib
import base64
import logging

import json  # for easy parsing
from pathlib import Path
from pprint import pprint

import numpy as np
import requests  # for HTTP requests

from splash_ingest.ingestors import MappedHD5Ingestor
from splash_ingest.model import Mapping


import h5py





class ScicatTomo(object):
    # settables
    host = "noether.lbl.gov" # 
    baseurl = "http://" + host + "/api/v3/"
    # timeouts = (4, 8)  # we are hitting a transmission timeout...
    timeouts = None  # we are hitting a transmission timeout...
    sslVerify = False # do not check certificate
    username="ingestor" # default username
    password="aman"     # default password

    # You should see a nice, but abbreviated table here with the logbook contents.
    token = None # store token here
    settables = ['host', 'baseurl', 'timeouts', 'sslVerify', 'username', 'password', 'token']
    pid = None # gets set if you search for something
    entries = None # gets set if you search for something
    datasetType = "RawDatasets"
    datasetTypes = ["RawDatasets", "DerivedDatasets", "Proposals"]


    def __init__(self, **kwargs):
        # nothing to do
        for key, value in kwargs.items():
            assert key in self.settables, f"key {key} is not a valid input argument"
            setattr(self, key, value)
        # get token
        self.token = self.get_token(username=self.username, password=self.password)

    def get_token(self, username=None, password=None):
        if username is None: username = self.username
        if password is None: password = self.password
        """logs in using the provided username / password combination and receives token for further communication use"""
        logging.info("Getting new token ...")
        response = requests.post(
            self.baseurl + "Users/login",
            json={"username": username, "password": password},
            timeout=self.timeouts,
            stream=False,
            verify=self.sslVerify,
        )
        if not response.ok:
            logging.error(f'** Error received: {response}')
            err = response.json()["error"]
            logging.error(f'{err["name"]}, {err["statusCode"]}: {err["message"]}')
            sys.exit(1)  # does not make sense to continue here
            data = response.json()
            logging.error(f"Response: {data}")

        data = response.json()
        # print("Response:", data)
        token = data["id"]  # not sure if semantically correct
        logging.info(f"token: {token}")
        self.token = token # store new token
        return token


    def send_to_scicat(self, url, dataDict = None, cmd="post"):
        """ sends a command to the SciCat API server using url and token, returns the response JSON
        Get token with the getToken method"""
        if cmd == "post":
            response = requests.post(
                url,
                params={"access_token": self.token},
                json=dataDict,
                timeout=self.timeouts,
                stream=False,
                verify=self.sslVerify,
            )
        elif cmd == "delete":
            response = requests.delete(
                url, params={"access_token": self.token}, 
                timeout=self.timeouts, 
                stream=False,
                verify=self.sslVerify,
            )
        elif cmd == "get":
            response = requests.get(
                url,
                params={"access_token": self.token},
                json=dataDict,
                timeout=self.timeouts,
                stream=False,
                verify=self.sslVerify,
            )
        elif cmd == "patch":
            response = requests.patch(
                url,
                params={"access_token": self.token},
                json=dataDict,
                timeout=self.timeouts,
                stream=False,
                verify=self.sslVerify,
            )
        rdata = response.json()
        if not response.ok:
            err = response.json()["error"]
            logging.error(f'{err["name"]}, {err["statusCode"]}: {err["message"]}')
            logging.error("returning...")
            rdata = response.json()
            logging.error(f"Response: {json.dumps(rdata, indent=4)}")

        return rdata


    def get_file_size_from_path_obj(self, pathobj):
        filesize = pathobj.lstat().st_size
        return filesize


    def getFileChecksumFromPathObj(self, pathobj):
        with open(pathobj) as file_to_check:
            # pipe contents of the file through
            return hashlib.md5(file_to_check.read()).hexdigest()

    def clear_previous_attachments(self, datasetId, datasetType):
        # remove previous entries to avoid tons of attachments to a particular dataset. 
        # todo: needs appropriate permissions!
        self.get_entries(url = self.baseurl + "Attachments", whereDict = {"datasetId": str(datasetId)})
        for entry in self.entries:
            url = self.baseurl + f"Attachments/{urllib.parse.quote_plus(entry['id'])}"
            self.send_to_scicat(url, {}, cmd="delete")

    def add_data_block(self, datasetId = None, filename = None, datasetType="RawDatasets", clearPrevious = False):
        if clearPrevious:
            self.clear_previous_attachments(datasetId, datasetType)

        dataBlock = {
            # "id": pid,
            "size": self.get_file_size_from_path_obj(filename),
            "dataFileList": [
                {
                    "path": str(filename.absolute()),
                    "size": self.get_file_size_from_path_obj(filename),
                    "time": self.getFileModTimeFromPathObj(filename),
                    "chk": "",  # do not do remote: getFileChecksumFromPathObj(filename)
                    "uid": str(
                        filename.stat().st_uid
                    ),  # not implemented on windows: filename.owner(),
                    "gid": str(filename.stat().st_gid),
                    "perm": str(filename.stat().st_mode),
                }
            ],
            "ownerGroup": "BAM 6.5",
            "accessGroups": ["BAM", "BAM 6.5"],
            "createdBy": "datasetUpload",
            "updatedBy": "datasetUpload",
            "datasetId": datasetId,
            "updatedAt": datetime.datetime.isoformat(datetime.datetime.utcnow()) + "Z",
            "createdAt": datetime.datetime.isoformat(datetime.datetime.utcnow()) + "Z",
            # "createdAt": "",
            # "updatedAt": ""
        }
        url = self.baseurl + f"{datasetType}/{urllib.parse.quote_plus(datasetId)}/origdatablocks"
        logging.debug(url)
        resp = self.send_to_scicat(url, dataBlock)
        return resp


    def get_entries(self, url, whereDict = {}):
        # gets the complete response when searching for a particular entry based on a dictionary of keyword-value pairs
        resp = self.send_to_scicat(url, {"filter": {"where": whereDict}}, cmd="get")
        self.entries = resp
        return resp


    def get_pid(self, url, whereDict = {}, returnIfNone=0, returnField = 'pid'):
        # returns only the (first matching) pid (or proposalId in case of proposals) matching a given search request
        resp = self.get_entries(url, whereDict)
        if resp == []:
            # no raw dataset available
            pid = returnIfNone
        else:
            pid = resp[0][returnField]
        self.pid = pid
        return pid
        
    def add_thumbnail(self, datasetId = None, filename = None, datasetType="RawDatasets", clearPrevious = False):
        if clearPrevious:
            self.clear_previous_attachments(datasetId, datasetType)

        def encodeImageToThumbnail(filename, imType = 'jpg'):
            header = "data:image/{imType};base64,".format(imType=imType)
            with open(filename, 'rb') as f:
                data = f.read()
            dataBytes = base64.b64encode(data)
            dataStr = dataBytes.decode('UTF-8')
            return header + dataStr

        dataBlock = {
            "caption": filename.stem,
            "thumbnail" : encodeImageToThumbnail(filename),
            "datasetId": datasetId,
            "ownerGroup": "BAM 6.5",
        }

        url = self.baseurl + f"{datasetType}/{urllib.parse.quote_plus(datasetId)}/attachments"
        logging.debug(url)
        resp = requests.post(
                    url,
                    params={"access_token": self.token},
                    timeout=self.timeouts,
                    stream=False,
                    json = dataBlock,
                    verify=self.sslVerify,
                )
        return resp

def doRaw(self, scm: ScicatTomo, file_name, run_start, thumbnail=None):
    # scb = self.scb # for convenience
    # year, datasetName, lbEntry = self.getLbEntryFromFileName(filename)
    # # this sets scs.year, scs.datasetName, scs.lbEntry

    # logging.info(f" working on {filename}")
    # sciMeta = scb.h5GetDict(filename, sciMetadataKeyDict)
    # if str(lbEntry.sampleid).startswith("{}".format(year)):
    #     sampleId = str(lbEntry.sampleid)
    # else:
    #     sampleId = scb.h5Get(filename, "/entry1/sample/name")
    # # see if entry exists:
    # pid = scb.getPid( # changed from "RawDatasets" to "datasets" which should be agnostic
    #     scb.baseurl + "datasets", {"datasetName": datasetName}, returnIfNone=0
    # )
    # if (pid != 0) and self.deleteExisting:
    #     # delete offending item
    #     url = scb.baseurl + "RawDatasets/{id}".format(id=urllib.parse.quote_plus(pid))
    #     scb.sendToSciCat(url, {}, cmd="delete")
    #     pid = 0
    data = {  # model for the raw datasets as defined in the RawDatasets
        "owner": None,
        "contactEmail": "brian.pauw@bam.de",
        "createdBy": self.username,
        "updatedBy": self.username,
        "creationLocation": "SAXS002",
        "creationTime": None,
        "updatedAt": datetime.datetime.isoformat(datetime.datetime.utcnow()) + "Z",
        # "createdAt": datetime.datetime.isoformat(datetime.datetime.utcnow()) + "Z",
        # "creationTime": h5Get(filename, "/processed/process/date"),
        "datasetName": datasetName,
        "type": "raw",
        "instrumentId": "SAXS002",
        "ownerGroup": "BAM 6.5",
        "accessGroups": ["BAM", "BAM 6.5"],
        "proposalId": str(lbEntry.proposal),
        "dataFormat": "NeXus",
        "principalInvestigator": scb.h5Get(filename, "/entry1/sample/sampleowner"),
        "pid": pid,
        "size": 0,
        "sourceFolder": filename.parent.as_posix(),
        "size": scb.getFileSizeFromPathObj(filename),
        "scientificMetadata": sciMeta,
        "sampleId": str(sampleId),
    }
    urlAdd = "RawDatasets"
    
    # determine thumbnail: 
    # upload
    if thumbnail.exists():
        npid = self.uploadBit(pid = pid, urlAdd = urlAdd, data = data, attachFile = thumbnail)
    logging.info("* * * * adding datablock")
    self.scb.addDataBlock(npid, file_name, datasetType='datasets', clearPrevious=False)

def gen_ev_docs(scm: ScicatTomo):
    with open('/home/dylan/work/als-computing/splash-ingest/.scratch/832Mapping.json', 'r') as json_file:
        data = json.load(json_file)
    map = Mapping(**data)
    with h5py.File('/home/dylan/data/beamlines/als832/20210204_172932_ddd.h5', 'r') as h5_file:
        ingestor = MappedHD5Ingestor(
            map, 
            h5_file, 
            'root', 
            '/home/dylan/data/beamlines/als832/thumbs')
        for name, doc in ingestor.generate_docstream():
            if 'start' in name:
                doRaw(doc, scm)
            if 'descriptor' in name:
                pprint(doc)

scm = ScicatTomo()
gen_ev_docs(scm)
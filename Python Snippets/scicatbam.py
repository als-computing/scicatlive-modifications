# coding: utf-8

# author: Brian R. Pauw, I. Bressler
# date: 2019.12.05
# v2.5 adds image thumbnail uploads

# Uploads the raw and processed datafiles based on the logbook and the information in the actual files.
# based on the datasetUpload_v1p1 python notebook
# ==

# we need some libraries to run these things.

import sys
import numpy as np
import requests  # for HTTP requests
import json  # for easy parsing
from pathlib import Path
import h5py
import datetime
# import pandas
# import xlrd
import hashlib
import urllib
import base64
# from SAXSClasses import readLog
# import argparse
# import xraydb
import logging

class scicatBam(object):
    # settables
    host = "catamel.scicat65.ddnss.de" # 
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
        self.token = self.getToken(username=self.username, password=self.password)

    def getToken(self, username=None, password=None):
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


    def sendToSciCat(self, url, dataDict = None, cmd="post"):
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


    def h5Get(self, filename, h5path, default = 'none', leaveAsArray = False):
        with h5py.File(filename, "r") as h5f:
            try:
                val = h5f.get(h5path)[()]
                # print('type val {} at key {}: {}'.format(val, h5path, type(val)))
                if isinstance(val, np.ndarray) and (not leaveAsArray):
                    if val.size == 1:
                        val = np.array([val.squeeze()])[0]
                    else:
                        val = val.mean()
                if isinstance(val, np.int32):
                    val = int(val)  # this could go wrong...
                if isinstance(val, np.float32):
                    val = float(val)
            except TypeError:
                logging.warning("cannot get value from file path {}, setting to default".format(h5path))
                val = default
        return val


    def h5GetDict(self, filename, keyPaths):
        resultDict = {}
        for key, h5path in keyPaths.items():
            resultDict[key] = self.h5Get(filename, h5path)
        return resultDict


    def getFileModTimeFromPathObj(self, pathobj):
        # may only work on WindowsPath objects...
        # timestamp = pathobj.lstat().st_mtime
        return str(datetime.datetime.fromtimestamp(pathobj.lstat().st_mtime))


    def getFileSizeFromPathObj(self, pathobj):
        filesize = pathobj.lstat().st_size
        return filesize


    def getFileChecksumFromPathObj(self, pathobj):
        with open(pathobj) as file_to_check:
            # pipe contents of the file through
            return hashlib.md5(file_to_check.read()).hexdigest()

    def clearPreviousAttachments(self, datasetId, datasetType):
        # remove previous entries to avoid tons of attachments to a particular dataset. 
        # todo: needs appropriate permissions!
        self.getEntries(url = self.baseurl + "Attachments", whereDict = {"datasetId": str(datasetId)})
        for entry in self.entries:
            url = self.baseurl + f"Attachments/{urllib.parse.quote_plus(entry['id'])}"
            self.sendToSciCat(url, {}, cmd="delete")

    def addDataBlock(self, datasetId = None, filename = None, datasetType="RawDatasets", clearPrevious = False):
        if clearPrevious:
            self.clearPreviousAttachments(datasetId, datasetType)

        dataBlock = {
            # "id": pid,
            "size": self.getFileSizeFromPathObj(filename),
            "dataFileList": [
                {
                    "path": str(filename.absolute()),
                    "size": self.getFileSizeFromPathObj(filename),
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
        resp = self.sendToSciCat(url, dataBlock)
        return resp


    def getEntries(self, url, whereDict = {}):
        # gets the complete response when searching for a particular entry based on a dictionary of keyword-value pairs
        resp = self.sendToSciCat(url, {"filter": {"where": whereDict}}, cmd="get")
        self.entries = resp
        return resp


    def getPid(self, url, whereDict = {}, returnIfNone=0, returnField = 'pid'):
        # returns only the (first matching) pid (or proposalId in case of proposals) matching a given search request
        resp = self.getEntries(url, whereDict)
        if resp == []:
            # no raw dataset available
            pid = returnIfNone
        else:
            pid = resp[0][returnField]
        self.pid = pid
        return pid
        
    def addThumbnail(self, datasetId = None, filename = None, datasetType="RawDatasets", clearPrevious = False):
        if clearPrevious:
            self.clearPreviousAttachments(datasetId, datasetType)

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

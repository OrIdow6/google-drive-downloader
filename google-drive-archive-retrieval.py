#!/usr/bin/env python3
# This module is meant to hold functionality to interact programatically with the archived files
# SPDX-License-Identifier: AGPL-3.0-or-later

import requests
from functools import cache
import json
from pathlib import Path
from urllib.parse import urljoin
import re
from html import unescape
import logging

WBM_BASE_URL = 'https://web.archive.org/web/'

sess = requests.Session()

#logging.basicConfig(level=logging.DEBUG)

def get_from_wbm(url, follow_redirects=True, streaming_download=False):
	# TODO 429
	'''
	Get an URL from the WBM, returning either the resultant Requests response object or None if it is not found.
	
	follow_redirects - whether to follow captured redirects. WBM-internal redirects (viz. to new dates) are always followed
	
	streaming_download - whether to stream the downloaded file (https://2.python-requests.org/en/master/user/advanced/#streaming-requests)
	'''
	
	if follow_redirects:
		r = sess.get(WBM_BASE_URL + "2im_/" + url, stream=streaming_download)
	else:
		r = sess.get(WBM_BASE_URL + "2im_/" + url, allow_redirects=False)
		# Keep iterating through redirects, as long as they look at WBM-internal ones
		while True: # TODO this does not work - also stream is not implemented here
			if "x-archive-redirect-reason" in r.headers and r.headers["x-archive-redirect-reason"].startswith("found capture at"):
				r = sess.get(urljoin(r.url, r.headers["location"]), allow_redirects=False)
	if any((s.startswith("x-archive-orig") for s in r.headers.keys())):
		return r
	else:
		return None

class GoogleDriveFolder:
	def __init__(self, folder_id: str, description_json = None):
		self._fid = folder_id
		if description_json:
			self._json = description_json
		else:
			r = self._get_info_json_raw()
			self._json = r # TODO check for error
	
	def _get_info_json_raw(self):
		r = get_from_wbm("https://clients6.google.com/drive/v2beta/files/" + self._fid + "?openDrive=false&reason=1001&syncType=0&errorRecovery=false&fields=kind%2CmodifiedDate%2CmodifiedByMeDate%2ClastViewedByMeDate%2CfileSize%2Cowners(kind%2CpermissionId%2Cid)%2ClastModifyingUser(kind%2CpermissionId%2Cid)%2ChasThumbnail%2CthumbnailVersion%2Ctitle%2Cid%2CresourceKey%2Cshared%2CsharedWithMeDate%2CuserPermission(role)%2CexplicitlyTrashed%2CmimeType%2CquotaBytesUsed%2Ccopyable%2CfileExtension%2CsharingUser(kind%2CpermissionId%2Cid)%2Cspaces%2Cversion%2CteamDriveId%2ChasAugmentedPermissions%2CcreatedDate%2CtrashingUser(kind%2CpermissionId%2Cid)%2CtrashedDate%2Cparents(id)%2CshortcutDetails(targetId%2CtargetMimeType%2CtargetLookupStatus)%2Ccapabilities(canCopy%2CcanDownload%2CcanEdit%2CcanAddChildren%2CcanDelete%2CcanRemoveChildren%2CcanShare%2CcanTrash%2CcanRename%2CcanReadTeamDrive%2CcanMoveTeamDriveItem)%2Clabels(starred%2Ctrashed%2Crestricted%2Cviewed)&supportsTeamDrives=true&retryCount=0&key=AIzaSyC1qbk75NzWBvSaDh6KnsjjA9pIrP4lYIE")
		if r is None:
			return None
		else:
			return r.json()
	
	def _list_folder(self):
		to_get = "/drive/v2beta/files?openDrive=false&reason=102&syncType=0&errorRecovery=false&q=trashed%20%3D%20false%20and%20'" + self._fid + "'%20in%20parents&fields=kind%2CnextPageToken%2Citems(kind%2CmodifiedDate%2CmodifiedByMeDate%2ClastViewedByMeDate%2CfileSize%2Cowners(kind%2CpermissionId%2Cid)%2ClastModifyingUser(kind%2CpermissionId%2Cid)%2ChasThumbnail%2CthumbnailVersion%2Ctitle%2Cid%2CresourceKey%2Cshared%2CsharedWithMeDate%2CuserPermission(role)%2CexplicitlyTrashed%2CmimeType%2CquotaBytesUsed%2Ccopyable%2CfileExtension%2CsharingUser(kind%2CpermissionId%2Cid)%2Cspaces%2Cversion%2CteamDriveId%2ChasAugmentedPermissions%2CcreatedDate%2CtrashingUser(kind%2CpermissionId%2Cid)%2CtrashedDate%2Cparents(id)%2CshortcutDetails(targetId%2CtargetMimeType%2CtargetLookupStatus)%2Ccapabilities(canCopy%2CcanDownload%2CcanEdit%2CcanAddChildren%2CcanDelete%2CcanRemoveChildren%2CcanShare%2CcanTrash%2CcanRename%2CcanReadTeamDrive%2CcanMoveTeamDriveItem)%2Clabels(starred%2Ctrashed%2Crestricted%2Cviewed))%2CincompleteSearch&appDataFilter=NO_APP_DATA&spaces=drive&maxResults=50&supportsTeamDrives=true&includeItemsFromAllDrives=true&corpora=default&orderBy=folder%2Ctitle_natural%20asc&retryCount=0&key=AIzaSyC1qbk75NzWBvSaDh6KnsjjA9pIrP4lYIE"
		while True:
			r = get_from_wbm("https://clients6.google.com" + to_get)
			j = r.json()
			
			for item in j["items"]:
				yield item
			
			if "nextPageToken" not in j:
				break
			else:
				nextPageToken = j["nextPageToken"]
				to_get = "/drive/v2beta/files?openDrive=false&reason=102&syncType=0&errorRecovery=false&q=trashed%20%3D%20false%20and%20'" + self._fid + "'%20in%20parents&fields=kind%2CnextPageToken%2Citems(kind%2CmodifiedDate%2CmodifiedByMeDate%2ClastViewedByMeDate%2CfileSize%2Cowners(kind%2CpermissionId%2Cid)%2ClastModifyingUser(kind%2CpermissionId%2Cid)%2ChasThumbnail%2CthumbnailVersion%2Ctitle%2Cid%2CresourceKey%2Cshared%2CsharedWithMeDate%2CuserPermission(role)%2CexplicitlyTrashed%2CmimeType%2CquotaBytesUsed%2Ccopyable%2CfileExtension%2CsharingUser(kind%2CpermissionId%2Cid)%2Cspaces%2Cversion%2CteamDriveId%2ChasAugmentedPermissions%2CcreatedDate%2CtrashingUser(kind%2CpermissionId%2Cid)%2CtrashedDate%2Cparents(id)%2CshortcutDetails(targetId%2CtargetMimeType%2CtargetLookupStatus)%2Ccapabilities(canCopy%2CcanDownload%2CcanEdit%2CcanAddChildren%2CcanDelete%2CcanRemoveChildren%2CcanShare%2CcanTrash%2CcanRename%2CcanReadTeamDrive%2CcanMoveTeamDriveItem)%2Clabels(starred%2Ctrashed%2Crestricted%2Cviewed))%2CincompleteSearch&appDataFilter=NO_APP_DATA&spaces=drive&pageToken=" + j["nextPageToken"] + "&maxResults=50&supportsTeamDrives=true&includeItemsFromAllDrives=true&corpora=default&orderBy=folder%2Ctitle_natural%20asc&retryCount=0&key=AIzaSyC1qbk75NzWBvSaDh6KnsjjA9pIrP4lYIE"
	
	def iter_children(self):
		for child in self._list_folder():
			if child["mimeType"] == "application/vnd.google-apps.folder":
				yield GoogleDriveFolder(child["id"], child)
			else:
				yield GoogleDriveFile(child["id"], child)
	
	@property
	def fid(self):
		return self._json["id"]
	
	@property
	def title(self):
		return self._json["title"]
		
	
	def __iter__(self):
		return self.iter_children()
	
	def __str__(self):
		return f"Google Drive folder {self.fid}: \"{self.title}\""


class GoogleDriveFile:
	
	def __init__(self, file_id: str, metadata_json=None):
		assert re.fullmatch(r'[a-zA-Z0-9_-]+', file_id)
		
		self._fid = file_id
		
		if metadata_json:
			if isinstance(metadata_json, str):
				metadata_json = json.loads(metadata_json)
			self._json = metadata_json
			self._file_not_in_archive = False
			if "error" in metadata_json:
				if metadata_json["error"].get("code") == 404:
					self._404_at_archive_time = True
					self._misc_error_during_archive = False
				else:
					self._404_at_archive_time = False
					self._misc_error_during_archive = True
			else:
				self._404_at_archive_time = False
				self._misc_error_during_archive = False
		else:
			r = self._get_info_json_raw()
			self._file_not_in_archive = r == None
			self._404_at_archive_time = r is not None and r.status_code == 404
			self._misc_error_during_archive = r is not None and r.status_code != 200
			self._json = r.json() if r is not None else None
		
		self._logger = logging.getLogger("ai.file." + self._fid)
	
	def _get_info_json_raw(self):
		'''
		Returns the raw Requests response representing the Google Drive info JSON response returned for this file.
		'''
		r = get_from_wbm("https://content.googleapis.com/drive/v2beta/files/" + self._fid + "?fields=kind%2CmodifiedDate%2CmodifiedByMeDate%2ClastViewedByMeDate%2CfileSize%2Cowners(kind%2CpermissionId%2Cid)%2ClastModifyingUser(kind%2CpermissionId%2Cid)%2ChasThumbnail%2CthumbnailVersion%2Ctitle%2Cid%2CresourceKey%2Cshared%2CsharedWithMeDate%2CuserPermission(role)%2CexplicitlyTrashed%2CmimeType%2CquotaBytesUsed%2Ccopyable%2CfileExtension%2CsharingUser(kind%2CpermissionId%2Cid)%2Cspaces%2Cversion%2CteamDriveId%2ChasAugmentedPermissions%2CcreatedDate%2CtrashingUser(kind%2CpermissionId%2Cid)%2CtrashedDate%2Cparents(id)%2CshortcutDetails(targetId%2CtargetMimeType%2CtargetLookupStatus)%2Ccapabilities(canCopy%2CcanDownload%2CcanEdit%2CcanAddChildren%2CcanDelete%2CcanRemoveChildren%2CcanShare%2CcanTrash%2CcanRename%2CcanReadTeamDrive%2CcanMoveTeamDriveItem)%2Clabels(starred%2Ctrashed%2Crestricted%2Cviewed)&supportsTeamDrives=true&includeBadgedLabels=true&enforceSingleParent=true&key=AIzaSyC1qbk75NzWBvSaDh6KnsjjA9pIrP4lYIE")
		return r
	
	@property
	def exists(self):
		'''
		Whether the file exists, i.e. whether it has been recorded in the archive.
		False may mean either that the file was deleted from Google Drive; or that it was not archived.
		'''
		return self.get_info_json_raw() is not None and self.get_info_json_raw().status_code == 200

	
	def is_probably_downloadable(self, give_reason=False):
		'''
		Whether, by the criteria used during the download process, the file is probably not excluded from the WBM.
		
		If give_reason is false, a boolean is returned. If true, a 2-tuple (value, human-readable explanation) is returned.
		'''
		
		# I realize I'm shadowing the module
		json = self._json
		if self._file_not_in_archive:
			retval = False
			reason = "File not found in archive."
		elif self._404_at_archive_time:
			retval = False
			reason = "Archive was run over the file, but the file did not exist at the time of archiving."
		elif self._misc_error_during_archive:
			retval = False
			reason = "Error at time of archiving."
		elif "fileSize" not in json or json["mimeType"] == "application/vnd.google-apps.document" or json["mimeType"] == "application/vnd.google-apps.spreadsheet":
				retval = False
				reason = "Google Docs, Sheets, etc. were not downloaded"
		elif self.mime_type.startswith("video/"):
			retval = False
			reason = "Video files were not downloaded."
		# TODO restrictred files
		else:
			retval = True
			reason = "There was no programmed restriction on downloading this file."
		if give_reason:
			return retval, reason
		else:
			return retval
	
	@property
	def title(self):
		return self._json["title"]
	
	@property
	def mime_type(self):
		return self._json["mimeType"]
	
	@property
	def file_size(self):
		return self._json["fileSize"]
	
	def get_download_response(self):
		'''
		Returns a Requests response object with a streaming download (https://2.python-requests.org/en/master/user/advanced/#streaming-requests) of the file's body.
		
		If you just want to download a file, save_body_to_path does this for you.
		'''
		
		# Either it will redirect directly to the download; or it will be a confirm page
		r = get_from_wbm("https://drive.google.com/uc?id=" + self._fid, streaming_download=True)
		if r is None:
			return None
		# TODO will need to have a quota check
		assert r.status_code == 200
		# Still on the DL page?
		if re.match(re.escape(WBM_BASE_URL) + r'\d+im_/' + r'https?://drive\.google\.com/uc\?id=', r.url):
			body = ""
			for chunk in r.iter_content(1024 * 1024, decode_unicode=True):
				body += chunk
			confirmed_url = re.search(r'href="(?P<url>/uc\?export=download&amp;confirm=[a-zA-Z0-9%-_]+&amp;id=[a-zA-Z0-9%-_]+)">Download anyway', body).group("url")
			confirmed_url = unescape(confirmed_url)
			r = get_from_wbm(urljoin("https://drive.google.com/uc?id=" + self._fid, confirmed_url), streaming_download=True)
		return r
		
		
	
	def save_body_to_path(self, path, skip_if_size_matches=False):
		'''
		Download the body and write it to the file given by path.
		'''
		path = Path(path) # If path is a string, wrap it; if already a Path, noop
		if skip_if_size_matches:
			if path.exists() and path.is_file():
				if path.stat().st_size == int(self._json["fileSize"]):
					return
		dlr = self.get_download_response()
		if dlr is None:
			raise FileNotFoundError(f"Cannot find a saved response for this file (id={self._fid}).")
		with path.open("wb") as f:
			for chunk in dlr.iter_content(1024 * 1024):
				self._logger.debug("Writing chunk to file")
				f.write(chunk)
	
	def __str__(self):
		return f'Google Drive file {self._fid}: "{self.title}"'

import os
def recursive_download(folder, path=""):
	def sanatize_name(name):
		return "".join(filter(lambda c: c != "/" and c != '\x00', name))[:255]
	
	path = path + sanatize_name(folder.title) + "/"
	
	print("DLing at " + path)
	os.makedirs(path, exist_ok=True)

	for item in folder:
		if isinstance(item, GoogleDriveFile):
			item_path = path + sanatize_name(item.title)
			if not item.is_probably_downloadable():
				_, reason = item.is_probably_downloadable(True)
				print("Excluding item {item_path} because {reason}")
			else:
				try:
					item.save_body_to_path(item_path, True)
				except FileNotFoundError:
					print(f"Error saving file ({item}) - download not found")
		else:
			recursive_download(item, path)
	
	
if __name__ == "__main__":
	import sys
	if len(sys.argv) != 2:
		print("Usage: ./google-drive-archive-retrieval.py [the file or folder ID you want to download to the current directory]")
	else:
		recursive_download(GoogleDriveFolder(sys.argv[1]))

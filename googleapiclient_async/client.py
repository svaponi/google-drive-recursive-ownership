import json
import logging

import googleapiclient.errors
import httplib2
import httpx
import oauth2client.client


class _HttpError(googleapiclient.errors.HttpError):

    def __init__(self, response: httpx.Response):
        self.resp = response
        self.content = response.content
        self.uri = response.request.url
        self.error_details = ""
        self.reason = self._get_reason()

    def _get_reason(self):
        """Calculate the reason for the error from the response content."""
        reason = None
        try:
            try:
                data = json.loads(self.content.decode("utf-8"))
            except json.JSONDecodeError:
                # In case it is not json
                data = self.content.decode("utf-8")
            if isinstance(data, dict):
                reason = data["error"]["message"]
                error_detail_keyword = next(
                    (
                        kw
                        for kw in ["detail", "details", "errors", "message"]
                        if kw in data["error"]
                    ),
                    "",
                )
                if error_detail_keyword:
                    self.error_details = data["error"][error_detail_keyword]
            elif isinstance(data, list) and len(data) > 0:
                first_error = data[0]
                reason = first_error["error"]["message"]
                if "details" in first_error["error"]:
                    self.error_details = first_error["error"]["details"]
            else:
                self.error_details = data
        except (ValueError, KeyError, TypeError):
            pass
        if reason is None:
            reason = ""
        return reason.strip()

    @property
    def status_code(self):
        return self.resp.status_code

    def __repr__(self):
        if self.error_details:
            return '<HttpError %s when requesting %s returned "%s". Details: "%s">' % (
                self.status_code,
                self.uri,
                self.reason,
                self.error_details,
            )
        elif self.uri:
            return '<HttpError %s when requesting %s returned "%s">' % (
                self.status_code,
                self.uri,
                self.reason,
            )
        else:
            return '<HttpError %s "%s">' % (self.status_code, self.reason)

    __str__ = __repr__


class GoogleDriveAsyncClient:
    def __init__(self, credentials: oauth2client.client.Credentials):
        self.baseurl = "https://www.googleapis.com/drive/v2"
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.client = None
        self.credentials = credentials
        self.credentials.refresh(httplib2.Http())

    async def list_files(self, folder_id: str = None):
        folder_id = folder_id or "root"
        return await self._send(url=f"{self.baseurl}/files/{folder_id}/children", method="GET")

    async def get_file(self, file_id: str):
        return await self._send(url=f"{self.baseurl}/files/{file_id}", method="GET")

    async def get_permissions(self, file_id: str, permission_id: str):
        return await self._send(url=f"{self.baseurl}/files/{file_id}/permissions/{permission_id}", method="GET")

    async def update_permissions(self, file_id: str, permission_id: str, body: dict, transfer_ownership: bool):
        return await self._send(url=f"{self.baseurl}/files/{file_id}/permissions/{permission_id}", method="PUT", body=body, params={"transferOwnership": transfer_ownership})

    async def get_permissions_by_email(self, email: str):
        return await self._send(url=f"{self.baseurl}/permissionIds/{email}", method="GET")

    async def _send(self, method: str, url: str, body: dict = None, params: dict = None, headers: dict = None):
        headers = headers or {}
        # refresh credentials if necessary
        self.credentials.apply(headers)
        request = self.client.build_request(url=url, method=method, headers=headers, params=params, json=body)
        response = await self.client.send(request)
        if not response.is_success:
            _line = f"{request.method} {request.url} >> {response.status_code} {response.text}"
            print(_line)
            raise _HttpError(response)
        result = response.json()
        return result

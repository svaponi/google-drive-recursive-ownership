#!/usr/bin/python
import asyncio
import os
import sys

import googleapiclient.discovery
import googleapiclient.errors
import googleapiclient.http
import oauth2client.client
import six

from googleapiclient_async.client import GoogleDriveAsyncClient

OAUTH2_SCOPE = "https://www.googleapis.com/auth/drive"
CLIENT_SECRETS = f"client_secrets.json"
CLIENT_CREDENTIALS = f"cred.json"


def get_credentials() -> oauth2client.client.Credentials:
    _current_dir = os.path.dirname(__file__)
    client_secrets = f"{_current_dir}/{CLIENT_SECRETS}"
    client_credentials = f"{_current_dir}/{CLIENT_CREDENTIALS}"
    if os.path.exists(client_credentials):
        try:
            with open(client_credentials, "r") as f:
                return oauth2client.client.Credentials.new_from_json(f.read())
        except Exception as e:
            print("Cannot reuse credentials due to error: {}".format(e))
    flow = oauth2client.client.flow_from_clientsecrets(client_secrets, OAUTH2_SCOPE)
    flow.redirect_uri = oauth2client.client.OOB_CALLBACK_URN
    authorize_url = flow.step1_get_authorize_url()
    print("Use this link for authorization: {}".format(authorize_url))
    code = six.moves.input("Verification code: ").strip()
    credentials: oauth2client.client.Credentials = flow.step2_exchange(code)
    with open(client_credentials, "w") as f:
        f.write(credentials.to_json())
    return credentials


def get_drive_service() -> GoogleDriveAsyncClient:
    credentials = get_credentials()
    return GoogleDriveAsyncClient(credentials)


async def get_permission_id_for_email(service: GoogleDriveAsyncClient, email):
    try:
        id_resp = await service.get_permissions_by_email(email=email)
        return id_resp["id"]
    except googleapiclient.errors.HttpError as e:
        print("An error occured: {}".format(e))


async def grant_ownership(service: GoogleDriveAsyncClient, drive_item, prefix, permission_id, show_already_owned):
    full_path = os.path.join(os.path.sep.join(prefix), drive_item["title"]).encode(
        "utf-8", "replace"
    )

    # pprint.pprint(drive_item)

    current_user_owns = False
    for owner in drive_item["owners"]:
        if owner["permissionId"] == permission_id:
            if show_already_owned:
                print("Item {} already has the right owner.".format(full_path))
            return
        elif owner["isAuthenticatedUser"]:
            current_user_owns = True

    print("Item {} needs ownership granted.".format(full_path))

    if not current_user_owns:
        print("    But, current user does not own the item.".format(full_path))
        return

    try:
        permission = await service.get_permissions(file_id=drive_item["id"], permission_id=permission_id)
        permission["role"] = "writer"
        permission["pendingOwner"] = "true"
        print("    Upgrading existing permissions to ownership.")
        return await service.update_permissions(
                file_id=drive_item["id"],
                permission_id=permission_id,
                body=permission,
                transfer_ownership=True,
            )
    except googleapiclient.errors.HttpError as e:
        if e.resp.status != 404:
            print("An error occurred updating ownership permissions: {}".format(e))
            return
    #
    # print("    Creating new ownership permissions.")
    # permission = {"role": "owner", "type": "user", "id": permission_id}
    # try:
    #     await service.insert_permissions(
    #         fileId=drive_item["id"],
    #         body=permission,
    #         emailMessage="Automated recursive transfer of ownership.",
    #     )
    # except googleapiclient.errors.HttpError as e:
    #     print("An error occurred inserting ownership permissions: {}".format(e))

async def process_one_file(
    service: GoogleDriveAsyncClient,
    file_id: str,
    callback=None,
    callback_args=None,
    minimum_prefix=None,
    current_prefix=None,
):
    if minimum_prefix is None:
        minimum_prefix = []
    if current_prefix is None:
        current_prefix = []
    if callback_args is None:
        callback_args = []

    item = await service.get_file(file_id=file_id)
    # pprint.pprint(item)
    if item["kind"] == "drive#file":
        if current_prefix[: len(minimum_prefix)] == minimum_prefix:
            _segments = current_prefix + [item["title"]]
            print(
                "File: {} ({})".format(
                    os.path.sep.join(_segments), item["id"]
                )
            )
            await callback(service, item, current_prefix, **callback_args)

        if item["mimeType"] == "application/vnd.google-apps.folder":
            next_prefix = current_prefix + [item["title"]]
            comparison_length = min(len(next_prefix), len(minimum_prefix))
            if (
                minimum_prefix[:comparison_length]
                == next_prefix[:comparison_length]
            ):
                await process_all_files(
                    service,
                    callback,
                    callback_args,
                    minimum_prefix,
                    next_prefix,
                    item["id"],
                )
            else:
                print(
                    "Ignore folder: {} ({})".format(
                        os.path.sep.join(next_prefix), item["id"]
                    )
                )


async def process_all_files(
    service: GoogleDriveAsyncClient,
    callback=None,
    callback_args=None,
    minimum_prefix=None,
    current_prefix=None,
    folder_id="root",
):
    if minimum_prefix is None:
        minimum_prefix = []
    if current_prefix is None:
        current_prefix = []
    if callback_args is None:
        callback_args = []

    print("Listing: {} ...".format(os.path.sep.join(current_prefix)))

    page_token = None
    while True:
        try:
            param = {}
            if page_token:
                param["pageToken"] = page_token
            children = await service.list_files(folder_id=folder_id)
            tasks = []
            for child in children.get("items", []):
                tasks.append(process_one_file(service, callback=callback, callback_args=callback_args, minimum_prefix=minimum_prefix, current_prefix=current_prefix, file_id=child["id"]) )
            await asyncio.gather(*tasks)
            page_token = children.get("nextPageToken")
            if not page_token:
                break
        except googleapiclient.errors.HttpError as e:
            print("An error occurred: {}".format(e))
            break


async def main():
    if len(sys.argv) < 3:
        raise ValueError(
            "Missing args, see https://github.com/svaponi/google-drive-recursive-ownership?tab=readme-ov-file#usage"
        )
    minimum_prefix = six.text_type(sys.argv[1])
    new_owner = six.text_type(sys.argv[2])
    show_already_owned = (
        False if len(sys.argv) > 3 and six.text_type(sys.argv[3]) == "false" else True
    )
    print(f'Changing all files at path "{minimum_prefix}" to owner "{new_owner}"')
    minimum_prefix_split = minimum_prefix.split(os.path.sep)
    print(f"Prefix: {minimum_prefix}")
    service = get_drive_service()
    permission_id = await get_permission_id_for_email(service, new_owner)
    print(f"User {new_owner} is permission ID {permission_id}.")
    await process_all_files(
        service,
        grant_ownership,
        {"permission_id": permission_id, "show_already_owned": show_already_owned},
        minimum_prefix_split,
    )
    print(
        f"Go to https://drive.google.com/drive/search?q=pendingowner:me (as {new_owner}), select all files, click 'Share' and accept ownership."
    )
    # print(files)


if __name__ == "__main__":
    asyncio.run(main())

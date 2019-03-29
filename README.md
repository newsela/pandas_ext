# pandas_ext
Python Pandas extensions for pandas dataframes


# Usage
```
import pandas_ext as px
```

## CSV

By default, pandas will natively read to s3 but won't write to s3.
```
px.read_csv
px.to_csv

```
## Excel

By default, pandas will natively read to s3 but won't write to s3.

To write to xls:
```bash
pip install pandas_ext[xls]
```

To write xlsx:
```bash
pip install pandas_ext[xlsx]
```

## Gdrive
By default, pandas does not read/write to Gdrive. 

### For G Suite administrators
At the organization level, one must do the necessary installation and
 deployment of the [gdrive lambda service](https://github.com/richiverse/gdrive-lambda/) to get this to work.

You will have to create a service account that shares your G Apps domain with the following APIs enabled:

- Google Drive

- Google Sheets

From there you must download the p12 credentials file and reference it in your settings.yml when deploying the gdrive service.

### For Gdrive clients
Once that is complete, you must share the folder you are interested in reading/writing to the service account email you've received from your administrator. 

Locally, for client access you must set the `GDRIVE_URL` and `GDRIVE_KEY` in your projects environment variable in order to talk to
the gdrive lambda service.


```
px.read_gdrive
px.to_gdrive
```

## Parquet
By default, pandas ~does not read/write to Parquet~. This has been added in pandas version 24 and my methods will eventually update to use them but still allow writing to s3.

```
px.read_parquet
px.to_parquet
```

## Spectrum
to_spectrum is unique to pandas_ext. 

```
px.to_spectrum
```

## Salesforce
salesforce methods are unique to pandas_ext.

```
px.read_sfdc
px.sfdc_metadata
px.patch_sfdc
px.async_patch_sfdc
```

## SQL service

```
px.read_sql
px.list_backends

## XML
Pandas doesn't natively support writing to XML format.
```
px.to_xml
```

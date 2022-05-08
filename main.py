sql = \
    """
    copy staging_events
    from '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    region '{}'
    """
json_path = "log.com"
file_format = "json '{}'".format(json_path)
igore = 1
files_csv = "IGNOREHEADER '{}' \n\tDELIMITER '{}'".format(json_path, igore)
sql_formatted = sql + file_format

print(sql_formatted)

print(sql + files_csv)
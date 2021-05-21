# from dataprep.eda import create_report
# import tempfile
# import logging

# OUTPUT_FILE_NAME = 'pandas_profile'

# def generate_report(
#     df, 
#     title="Pandas Profiling Report"
# ):

#     temp_directory = tempfile.TemporaryDirectory()
#     report = create_report(df, title=title)
#     report.save(filename=OUTPUT_FILE_NAME, to=temp_directory.name)
#     output_path = f'{temp_directory.name}/{OUTPUT_FILE_NAME}'
#     logging.info(f'Successfully loaded df profile to {output_path}')

#     return output_path
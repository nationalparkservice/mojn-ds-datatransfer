import arcpy
from arcpy import da
from datetime import datetime
import os
print("Python Script loaded!")

def download_visit_photos(attTable, photoFeatureClass, visitFeatureClass, dataPhotoLocation, originalsLocation, folder):
	# Dictionary to return (probably better to use pandas DataFrame but returning a DataFrame seems to crash R)
	photo_paths = {'GlobalID': [], 'VisitGUID': [], 'OriginalFilePath': [], 'RenamedFilePath': []}
	
	# Cursor for attachment table
	att_cursor = da.SearchCursor(attTable, ['DATA', 'ATT_NAME', 'ATTACHMENTID', 'REL_GLOBALID'])
	print(att_cursor)
	for item in att_cursor:
		fk_photo = item[3]  # Get global id for corresponding row in photo feature class
		# Cursor for data table
		photo_cursor = da.SearchCursor(photoFeatureClass, field_names = ['parentglobalid'], where_clause = "GLOBALID = " + "'" + fk_photo + "'")
		
		# Get lake code, and date, and use them to form a prefix for the filename
		for row in photo_cursor:  # There is actually just one row
			fk_visit = str(row[0])
			print(fk_visit)
		
		visit_cursor = da.SearchCursor(visitFeatureClass, field_names = ['SiteCode', 'DateTime'], where_clause = "GLOBALID = " + "'" + fk_visit + "'")  # Get global id for corresponding row in visit feature class
		for row in visit_cursor:  # There is actually just one row
			time = datetime.strftime(row[1], "%Y%m%d")
			site = visit_cursor[0]
			time_folder = datetime.strftime(row[1], "%Y_%m_%d")
			year = datetime.strftime(row[1], "%Y")
			prefix = site + "_" + time + "_" + folder

		attachment = item[0]
		att_id = str(item[2])
		filename = prefix + "_" + att_id.zfill(4) + ".jpg"  # zero-fill the attachment ID so that it is always 4 digits
		# Check if folders for spring and/or date exist. If not, create them
		data_photo_path = dataPhotoLocation + os.sep + site + os.sep + year
		orig_photo_path = originalsLocation + os.sep + time_folder
		# Put a copy of photos in incoming photos folder
		if not os.path.exists(orig_photo_path):
			os.makedirs(orig_photo_path)
		if not os.path.exists(orig_photo_path + os.sep + filename):
			f = open(orig_photo_path + os.sep + filename, 'wb')
			f.write(attachment.tobytes())
			f.close()
		  
		# Put a copy of photos in STLK data folder
		if not os.path.exists(data_photo_path):
			os.makedirs(data_photo_path)
		if not os.path.exists(data_photo_path + os.sep + filename):
			f = open(data_photo_path + os.sep + filename, 'wb')
			f.write(attachment.tobytes())
			f.close()
		# Add to list of orig and renamed file paths
		photo_paths['GlobalID'].append(fk_photo)
		photo_paths['VisitGUID'].append(fk_visit)
		photo_paths['OriginalFilePath'].append(orig_photo_path + os.sep + filename)
		photo_paths['RenamedFilePath'].append(data_photo_path + os.sep + filename)
	# Return orig file path, renamed file path, and GUID FK to visit
	return photo_paths

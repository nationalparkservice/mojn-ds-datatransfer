import arcpy
from arcpy import da
from datetime import datetime
import os
print("Python Script loaded!")

def download_visit_photos(attTable, photoFeatureClass, repeatFeatureClass, visitFeatureClass, dataPhotoLocation, originalsLocation, photoCodeDict, photoTypeField):
    # Dictionary to return (probably better to use pandas DataFrame but returning a DataFrame seems to crash R)
    photo_paths = {'GlobalID': [], 'RepeatGUID': [], 'VisitGUID': [], 'OriginalFilePath': [], 'RenamedFilePath': [], 'PhotoType': []}
    # Cursor for photo attachment table - start at lowest table and work up
    att_cursor = da.SearchCursor(attTable, ['DATA', 'ATT_NAME', 'ATTACHMENTID', 'REL_GLOBALID'])
    print("att_cursor =", att_cursor)
    
    for item in att_cursor:
        prefix=""
        fk_photo = item[3]  # Get global id for corresponding row in photo feature class
        # Cursor for photo data table (riparianVegInt/InternalCamera for example)
        photo_cursor = da.SearchCursor(photoFeatureClass, field_names = ['parentglobalid'], where_clause = "GLOBALID = " + "'" + fk_photo + "'")
        for row in photo_cursor: # there is actually just one row
            # get the parent global ID from the photo data table, as store as fk_visit ( i think this should be fk_repeat not visit.)
            fk_repeat = str(row[0])
            print("fk_repeat = ",fk_repeat)
            # get the parent global ID from the repeat data table (riparianVeg/VegImageRepeat), as store as fk_repeat
            # have to pass generic option here for the photo type field as it changes with each eature layer
            repeat_cursor = da.SearchCursor(repeatFeatureClass, field_names = [photoTypeField, 'parentglobalid'], where_clause = "GLOBALID = " +"'" + fk_repeat + "'")
            # now go use the parent globaliD to get to the visit record
            for row in repeat_cursor:
                fk_visit = str(row[1])
                print("fk_visit =",fk_visit)
                photo_type = photoCodeDict[row[0]]
                print("photo type=", photo_type)
        		# Get lake code, and date from visit table, and use them to form a prefix for the filename- join on fk_visit
                visit_cursor = da.SearchCursor(visitFeatureClass, field_names = ['SiteCode', 'DateTime'], where_clause = "GLOBALID = " + "'" + fk_visit + "'")
                for row in visit_cursor:
                    time = datetime.strftime(row[1], "%Y%m%d")
                    site = visit_cursor[0]
                    time_folder = datetime.strftime(row[1], "%Y_%m_%d")
                    year = datetime.strftime(row[1], "%Y")
                    prefix = site + "_" + time + "_" + photo_type           
        attachment = item[0]
        att_id = str(item[2])
        filename = prefix + "_" + att_id.zfill(4) + ".jpg"  # zero-fill the attachment ID so that it is always 4 digits
        # Check if folders for spring and/or date exist. If not, create them
        data_photo_path = dataPhotoLocation + os.sep + site
        orig_photo_path = originalsLocation + os.sep + time_folder
        # Put a copy of photos in incoming photos folder
        if not os.path.exists(orig_photo_path):
            os.makedirs(orig_photo_path)
        if not os.path.exists(orig_photo_path + os.sep + filename):
            f = open(orig_photo_path + os.sep + filename, 'wb')
            f.write(attachment.tobytes())
            f.close()
        # put a copy of photos in the DS folder
        if not os.path.exists(data_photo_path):
            os.makedirs(data_photo_path)
        if not os.path.exists(data_photo_path + os.sep + filename):
            f = open(data_photo_path + os.sep + filename, 'wb')
            f.write(attachment.tobytes())
            f.close()
        # add to list of orig and renamed files
        photo_paths['GlobalID'].append(fk_photo)
        photo_paths['RepeatGUID'].append(fk_repeat)
        photo_paths['VisitGUID'].append(fk_visit)
        photo_paths['OriginalFilePath'].append(orig_photo_path + os.sep + filename)
        photo_paths['RenamedFilePath'].append(data_photo_path + os.sep + filename)
        photo_paths['PhotoType'].append(photo_type)
    # return orig file path, renamed file path, and GUID FK to visit,and repeat, as well as the photo type
    return photo_paths
        
                
                    
                    
        

import arcpy, datetime, calendar, sys, re, shutil, os
import csv, xlrd, gc, traceback, operator, functools, string, xlwt
import ReportProcessingConfig as config


def log_error(customized_msg, logfile_handle):

    """
    function to print and log error message in logfile
    :param customized_msg: customized message added to log file
    :param logfile_handle: log file IO handler
    :return: NIL
    """
    tb = sys.exc_info()[2]
    tbinfo = traceback.format_tb(tb)[0]
    pymsg = "PYTHON ERRORS:\nTraceback Info:\n" + tbinfo + "\nError Info:\n     " + str(
        sys.exc_type) + ": " + str(sys.exc_value) + "\n"
    msgs = "ARCPY ERRORS:\n" + arcpy.GetMessages(2) + "\n"
    logfile_handle.writelines(customized_msg + str(msgs) + "\n" + pymsg + "\n")


def add_months(sourcedate, months):

    """
    function to calculate end date
    :param sourcedate: start date
    :param months: duration
    :return: end date in string
    """

    month = sourcedate.month - 1 + months
    year = sourcedate.year + month / 12
    month = month % 12 + 1
    day = min(sourcedate.day, calendar.monthrange(year, month)[1])
    end_date = datetime.date(year, month, day)
    return str(end_date.day) + "/" + str(end_date.month) + "/" + str(end_date.year)


def uptodigit(s):

    """
    function to check for letters in front of project lot number
    :param s:
    :return:
    """

    before_digit = ''
    i = 0
    while i < len(s) and not (s[i] in '0123456789'):
        before_digit = before_digit + s[i]
        i = i + 1
    return before_digit


def execute():

    """
    Execute BCA report processing job
    :return: NIL
    """
    arcpy.AddMessage("START BCA Processing")
    arcpy.env.workspace = config.temp_data_gdb
    arcpy.env.overwriteOutput = True
    sys.path.append(config.notif_system_script_folder)

    # Other Variables
    arcpy.AddMessage("Import toolbox")
    arcpy.ImportToolbox(config.notif_toolbox)
    REGEX_FOR_INVALID_CHARS = re.compile(r'[^0-9a-zA-Z]+')
    todayDate = datetime.datetime.now().strftime("%Y%m%d")
    logFile = file(
        config.report_processing_log + "\\" + todayDate + "_NotificationSystemLog" + ".txt", "a")


    # get all unzipped files uploaded to shared folder
    configfiles = [os.path.join(dirpath, f)
                   for dirpath, dirnames, files in os.walk(config.SharedFolder)
                   for f in files if f.endswith('.csv') or f.endswith('.xls') or f.endswith('.xlsx') or f.endswith('.XLS')]

    correct_config_files = [f for f in configfiles if "\BCAWeeklyPermitReport\\" in f]

    # PREPARE workspace
    arcpy.AddMessage("Preparing workspace...")
    for BCAreport in correct_config_files:

        input_file_name = BCAreport.split("\\")[-1]

        MukimConstruct = arcpy.SearchCursor(config.MukimConstructSource)
        PermitDateExists = False

        for row in MukimConstruct:
            aux = input_file_name[:8]
            if "CORRECTED" not in BCAreport.upper():
                filedate = datetime.datetime.strptime(aux, "%Y%m%d")
            else:
                clean_filename = input_file_name.split(".")[0]
                filedate = datetime.datetime.strptime(clean_filename[-8:], "%Y%m%d")
            if filedate == row.PERMIT_DATE and "CORRECTED" not in BCAreport.upper():
                PermitDateExists = True
                break
        if PermitDateExists and "CORRECTED" not in BCAreport.upper():
            PermitDateExistsLog = file(
                config.ErrorLogFolder + "\\" + input_file_name.split(".")[0] +
                " file's Permit Date already exists" + ".log",
                "a")
            PermitDateExistsLog.write(
                "Permit Date for the file " + input_file_name + " already exists in Mukim Construct at " + str(
                    datetime.datetime.now()))
            logFile.writelines(
                "Permit Date for the file " + input_file_name + " already exists in Mukim Construct at " + str(
                    datetime.datetime.now()) + "\n")

        else:

            # 00. Creation of geodatabases that will serve as workspaces
            logFile.writelines("00 Creation of temp gdb starts at " + str(datetime.datetime.now()) + "\n")

            if arcpy.Exists(config.TempDataGDB):
                arcpy.Delete_management(config.TempDataGDB)
                arcpy.CreateFileGDB_management(config.Notification, "Temp_data.gdb")
            else:
                arcpy.CreateFileGDB_management(config.Notification, "Temp_data.gdb")

            if arcpy.Exists(config.SDEDataGDB):
                arcpy.Delete_management(config.SDEDataGDB)
                arcpy.CreateFileGDB_management(config.Notification, "Source.gdb")
            else:
                arcpy.CreateFileGDB_management(config.Notification, "Source.gdb")

            if arcpy.Exists(config.CurrentMukimConstructDataGDB):
                arcpy.Delete_management(config.CurrentMukimConstructDataGDB)
                arcpy.CreateFileGDB_management(config.Notification, "Final_data.gdb")
            else:
                arcpy.CreateFileGDB_management(config.Notification, "Final_data.gdb")

            logFile.writelines("00 Creation of temp gdb ends at " + str(datetime.datetime.now()) + "\n")

            # 01. Import the base data
            logFile.writelines("01 Import of base data starts at " + str(datetime.datetime.now()) + "\n")
            arcpy.FeatureClassToFeatureClass_conversion(config.MukimConstructSource, config.CurrentMukimConstructDataGDB,
                                                        "MUKIM_CONSTRUCT", "", "", "")
            arcpy.FeatureClassToFeatureClass_conversion(config.MukimConstructByProjSource, config.CurrentMukimConstructDataGDB,
                                                        "MUKIM_CONSTRUCT_BYPROJ", "", "", "")
            arcpy.FeatureClassToFeatureClass_conversion(config.DepotSource, config.SDEDataGDB, "DepotBoundary", "", "", "")
            arcpy.FeatureClassToFeatureClass_conversion(config.CatchmentSource, config.SDEDataGDB, "CatchmentBoundary", "", "", "")
            arcpy.FeatureClassToFeatureClass_conversion(config.LandlotSource, config.TempDataGDB, "Land_lot", "", "", "")
            # Calculate the lot key without letter
            arcpy.AddField_management(config.LandLot, "Lotkey_wo_letter", "TEXT", "", "", "10", "", "NULLABLE", "NON_REQUIRED",
                                      "")
            arcpy.CalculateField_management(config.LandLot, "Lotkey_wo_letter", "!lot_key![:10]", "PYTHON", "")

            logFile.writelines("01 Import of base data ends at " + str(datetime.datetime.now()) + "\n")


    # START THE LOOP TO PROCESS ALL THE FILES
    clcounter = 0

    if len(correct_config_files) == 0:
        logFile.writelines("No BCA report to process at " + str(datetime.datetime.now()) + "\n")

    arcpy.AddMessage("Processing files...")
    for BCAreport in configfiles:

        clcounter += 1
        arcpy.AddMessage(BCAreport)
        input_file_name = BCAreport.split("\\")[-1]
        MukimConstruct = arcpy.SearchCursor(config.MukimConstructSource)
        PermitDateExists = False

        # CHEKC FILE DATE EXISTS
        for row in MukimConstruct:
            aux = input_file_name[:8]
            if "CORRECTED" not in BCAreport.upper():
                filedate = datetime.datetime.strptime(aux, "%Y%m%d")
            else:
                clean_filename = input_file_name.split(".")[0]
                filedate = datetime.datetime.strptime(clean_filename[-8:], "%Y%m%d")
            if filedate == row.PERMIT_DATE and "CORRECTED" not in input_file_name.upper():
                PermitDateExists = True
                break

        HEADERVALID = True
        with xlrd.open_workbook(BCAreport) as wb:
            sh = wb.sheet_by_index(0)
            for r in range(sh.nrows):
                colcount = 0
                if sh.row_values(r)[colcount] == 'Error_Message':
                    HEADERVALID = True
                elif sh.row_values(r)[colcount] == 'Project Ref No' or sh.row_values(r)[colcount] == 'Project_Ref_No':
                    HEADERVALID = True
                else:
                    PermitDateExistsLog = file(config.ErrorLogFolder + "\\" + input_file_name.split(".")[
                        0] + " file's header format is not acceptable for processing" + ".log", "a")
                    PermitDateExistsLog.write(
                        "The header format for the file " + input_file_name + " is not acceptable for processing at " + str(
                            datetime.datetime.now()))
                    logFile.writelines(
                        "The header format for the file " + input_file_name + " is not acceptable for processing at " + str(
                            datetime.datetime.now()) + "\n")
                    HEADERVALID = False
                break

        if not PermitDateExists and HEADERVALID:
            logFile.writelines("Starts processing " + BCAreport + " at " + str(datetime.datetime.now()) + "\n")

            # Status update to run/not run the SiteInspection Update
            Log_SiteInspectionUpdate = file(config.SiteInspectionUpdate, "w")
            Log_SiteInspectionUpdate.writelines("NO")
            Log_SiteInspectionUpdate.close()

            # 02. Import the BCA report to a geodatabase table
            logFile.writelines("02 Import of table to gdb starts at " + str(datetime.datetime.now()) + "\n")

            try:
                if arcpy.Exists(config.TempDataGDB + "\\ConvertedBCAreport_02"):
                    arcpy.Delete_management(config.TempDataGDB + "\\ConvertedBCAreport_02")
                    arcpy.CreateTable_management(config.TempDataGDB, "ConvertedBCAreport_02", config.TemplateConvertedBCAreport)
                else:
                    arcpy.CreateTable_management(config.TempDataGDB, "ConvertedBCAreport_02", config.TemplateConvertedBCAreport)
                if arcpy.Exists(BCAreport[:-5] + '_err' + '.csv'):
                    # rename old error report
                    os.remove(BCAreport[:-5] + '_err' + '.csv')
                else:
                    result = "Error file does not exist"
                if BCAreport.endswith('.xls') or BCAreport.endswith('.xlsx') or BCAreport.endswith('.XLS'):
                    rows_out = arcpy.InsertCursor(config.BCAReportGDBTable)
                    fldlist = arcpy.ListFields(config.BCAReportGDBTable)
                    fldlist.pop(0)
                    with xlrd.open_workbook(BCAreport) as wb:
                        sh = wb.sheet_by_index(0)
                        for r in range(sh.nrows):
                            colcount = 0
                            if sh.row_values(r)[colcount] != 'Error_Message':
                                colcount = 0
                            else:
                                colcount = 1
                            break
                        for r in range(sh.nrows):
                            colcounter = colcount
                            if r > 0:
                                new_row_out = rows_out.newRow()
                                for efld in fldlist:
                                    if efld.name <> 'OBJECTID' and efld.name <> 'ConcatFields':
                                        new_row_out.setValue(efld.name, sh.row_values(r)[colcounter])
                                        colcounter += 1

                                logFile.writelines("Inserting: " + str(new_row_out) + "\n")
                                rows_out.insertRow(new_row_out)
                    del rows_out, new_row_out

                elif BCAreport.endswith('.csv'):

                    BCAreportread = csv.DictReader(open(BCAreport, 'rb'), delimiter=',', quotechar='"')
                    rows_out = arcpy.InsertCursor(config.BCAReportGDBTable)
                    for attribute in BCAreportread:
                        new_row_out = rows_out.newRow()
                        new_row_out.Project_Ref_No = attribute['Project_Ref_No']
                        new_row_out.Project_Title = attribute['Project_Title']
                        new_row_out.House_Blk_No = attribute['House_Blk_No']
                        new_row_out.Road_Name = attribute['Road_Name']
                        new_row_out.Level_No = attribute['Level_No']
                        new_row_out.Unit_No = attribute['Unit_No']
                        new_row_out.Building_Name = attribute['Building_Name']
                        new_row_out.Postal_Code = attribute['Postal_Code']
                        new_row_out.Project_Mukim_nos = attribute['Project_Mukim_nos']
                        new_row_out.Project_Lot_nos = attribute['Project_Lot_nos']
                        new_row_out.Permit_Type_of_Work = attribute['Permit_Type_of_Work']
                        new_row_out.Type_of_Work = attribute['Type_of_Work']
                        new_row_out.Owner_s_name = attribute['Owners_name']
                        new_row_out.Owner_s_firm_name = attribute['Owners_firm_name']
                        new_row_out.Owner_s_address = attribute['Owners_address']
                        new_row_out.Owner_s_Tel_No = attribute['Owners_Tel_No']
                        new_row_out.Owner_s_Email_address = attribute['Owners_Email_address']
                        new_row_out.Builder_s_name = attribute['Builders_name']
                        new_row_out.Builder_s_firm_name = attribute['Builders_firm_name']
                        new_row_out.Builder_s_address = attribute['Builders_address']
                        new_row_out.Builder_s_Tel_No = attribute['Builders_Tel_No']
                        new_row_out.Builder_s_email_address = attribute['Builders_email_address']
                        new_row_out.PE_s_name = attribute['PEs_name']
                        new_row_out.PE_s_firm_name = attribute['PEs_firm_name']
                        new_row_out.PE_s_address = attribute['PEs_address']
                        new_row_out.PE_s_Tel_No = attribute['PEs_Tel_No']
                        new_row_out.PE_s_Email_address = attribute['PEs_Email_address']
                        new_row_out.Architect_s_name = attribute['Architects_name']
                        new_row_out.Architect_s_firm_name = attribute['Architects_firm_name']
                        new_row_out.Architect_s_address = attribute['Architects_address']
                        new_row_out.Architect_s_Tel_No = attribute['Architects_Tel_No']
                        new_row_out.Architect_s_Email_address = attribute['Architects_Email_address']
                        new_row_out.Project_Cost = attribute['Project_Cost']
                        new_row_out.Project_Duration = attribute['Project_Duration']
                        new_row_out.Approval_Date_DD_MM_YYYY_ = attribute['Approval_Date']
                        rows_out.insertRow(new_row_out)
                    if new_row_out:
                        del new_row_out
                    if rows_out:
                        del rows_out

            except:
                log_error("Error in 02 Import of table to gdb: ", logFile)
            logFile.writelines("02 Import of table to gdb ends at " + str(datetime.datetime.now()) + "\n")

            # 03. Remove spaces in key fields for the concatenation
            logFile.writelines("03 Removing of spaces starts at " + str(datetime.datetime.now()) + "\n")

            try:
                rowsSpace = arcpy.UpdateCursor(config.BCAReportGDBTable)

                for row in rowsSpace:
                    ProjRef = row.Project_Ref_No.strip()
                    ProjMukim = row.Project_Mukim_nos.strip()
                    ProjLot = row.Project_Lot_nos.strip()
                    BuilderN = row.Builder_s_name.strip()
                    row.Project_Ref_No = ProjRef
                    row.Project_Mukim_nos = ProjMukim
                    row.Project_Lot_nos = ProjLot
                    row.Builder_s_name = BuilderN
                    rowsSpace.updateRow(row)
                if row:
                    del row
                if rowsSpace:
                    del rowsSpace
            except:
                log_error("Error in 03 Removing of spaces: ", logFile)
            logFile.writelines("03 Removing of spaces ends at " + str(datetime.datetime.now()) + "\n")

            # 04. Concatenate Project_Ref_No, Project_Mukim_nos, Project_Lot_nos, Builder_s_name
            logFile.writelines("04 Concatenate the three fields starts at " + str(datetime.datetime.now()) + "\n")

            try:
                rows = arcpy.UpdateCursor(config.BCAReportGDBTable)
                for row in rows:
                    expression = str(row.Project_Ref_No) + "-" + str(row.Project_Mukim_nos) + "-" + str(
                        row.Project_Lot_nos) + "-" + str(row.Builder_s_name)
                    row.ConcatFields = expression
                    rows.updateRow(row)
                if row:
                    del row
                if rows:
                    del rows

            except:
                log_error("Error in 04 Concatenate the three fields: ", logFile)
            logFile.writelines("04 Concatenate the three fields ends at " + str(datetime.datetime.now()) + "\n")

            # 05. Create temporary tables for Unique and Duplicate records
            logFile.writelines("05 Create temporary tables starts at " + str(datetime.datetime.now()) + "\n")

            try:
                if arcpy.Exists(config.TempDataGDB + "\\Uniquerows"):
                    arcpy.Delete_management(config.TempDataGDB + "\\Uniquerows")
                    arcpy.CreateTable_management(config.TempDataGDB, "Uniquerows", config.TemplateConcat, "")
                else:
                    arcpy.CreateTable_management(config.TempDataGDB, "Uniquerows", config.TemplateConcat, "")

                if arcpy.Exists(config.TempDataGDB + "\\Duplicaterows"):
                    arcpy.Delete_management(config.TempDataGDB + "\\Duplicaterows")
                    arcpy.CreateTable_management(config.TempDataGDB, "Duplicaterows", config.TemplateConcat, "")
                else:
                    arcpy.CreateTable_management(config.TempDataGDB, "Duplicaterows", config.TemplateConcat, "")
            except:
                log_error("Error in 05 Create temporary tables: ", logFile)
            logFile.writelines("05 Create temporary tables ends at " + str(datetime.datetime.now()) + "\n")

            # 06. Separate unique and duplicate records
            logFile.writelines("06 Separate unique and duplicate rows starts at " + str(datetime.datetime.now()) + "\n")

            try:
                print "Start step  06"
                rows_inCB02 = arcpy.UpdateCursor(config.BCAReportGDBTable)
                rows_outUnique = arcpy.InsertCursor(config.UniqueRecords)
                # print rows_outUnique
                rows_outDuplicate = arcpy.InsertCursor(config.DuplicateRecords)

                rows_unique = []
                rows_duplicates = []
                for row in rows_inCB02:
                    if row.ConcatFields not in rows_unique:
                        rows_unique = rows_unique + [row.ConcatFields]
                    else:
                        rows_duplicates = rows_duplicates + [row.ConcatFields]

                print "Start step  06 1"
                for item in rows_unique:
                    print "clcounter: " + str(clcounter)
                    print "item: " + str(item)
                    newrow = rows_outUnique.newRow()
                    newrow.Concat = item
                    # print newrow
                    rows_outUnique.insertRow(newrow)

                print "Start step  06 2"
                for item in rows_duplicates:
                    print "clcounter: " + str(clcounter)
                    print "item: " + str(item)
                    newrow = rows_outDuplicate.newRow()
                    newrow.Concat = item
                    rows_outDuplicate.insertRow(newrow)

                print "Start step  06 3"

                if rows_inCB02:
                    del rows_inCB02
                if rows_outUnique:
                    del rows_outUnique
                if rows_outDuplicate:
                    del rows_outDuplicate
                if row:
                    del row
            except:
                log_error("Error in 06 Separate unique and duplicate rows: ", logFile)
            logFile.writelines("06 Separate unique and duplicate rows ends at " + str(datetime.datetime.now()) + "\n")

            # 07. Get the rest of the fields for Uniquerows table
            logFile.writelines(
                "07 Get the rest of the fields for unique rows starts at " + str(datetime.datetime.now()) + "\n")
            arcpy.env.workspace = config.TempDataGDB
            arcpy.AddMessage("Starting toolbox JoinUniqueRestofFields")

            try:
                arcpy.JoinUniqueRestofFields()
            except:
                log_error("Error in 07 Get the rest of the fields for unique rows: ", logFile)
            logFile.writelines(
                    "07 Get the rest of the fields for unique rows ends at " + str(datetime.datetime.now()) + "\n")

            # 08. Get the rest of the fields for Duplicaterows table
            logFile.writelines(
                "08 Get the rest of the fields for duplicate rows starts at " + str(datetime.datetime.now()) + "\n")
            arcpy.AddMessage("START toolbox JoinDuplicateRestofFields")
            try:
                arcpy.JoinDuplicateRestofFields()

            except:
                log_error("Error in 08 Get the rest of the fields for duplicate rows: ", logFile)

            logFile.writelines(
                "08 Get the rest of the fields for duplicate rows ends at " + str(datetime.datetime.now()) + "\n")

            # 09. Log duplicate records
            logFile.writelines("09 Log duplicate records starts at " + str(datetime.datetime.now()) + "\n")
            arcpy.AddMessage("Logging duplicate records")
            try:
                # Initialize the error log
                wbk = xlwt.Workbook()
                sheet = wbk.add_sheet('Book 1')
                row_count = 0
                col_count = 0
                header = ['Error_Message', 'Project_Ref_No', 'Project_Title', 'House_Blk_No', 'Road_Name', 'Level_No',
                          'Unit_No', 'Building_Name', 'Postal_Code', 'Project_Mukim_nos', 'Project_Lot_nos',
                          'Permit_Type_of_Work', 'Type_of_Work', 'Owners_name', 'Owners_firm_name', 'Owners_address',
                          'Owners_Tel_No', 'Owners_Email_address', 'Builders_name', 'Builders_firm_name',
                          'Builders_address', 'Builders_Tel_No', 'Builders_email_address', 'PEs_name', 'PEs_firm_name',
                          'PEs_address', 'PEs_Tel_No', 'PEs_Email_address', 'Architects_name', 'Architects_firm_name',
                          'Architects_address', 'Architects_Tel_No', 'Architects_Email_address', 'Project_Cost',
                          'Project_Duration', 'Approval_Date']
                for fieldname in header:
                    sheet.write(row_count, col_count, fieldname)
                    col_count += 1
                wbk.save(config.ErrorLogFolder + "\\" + input_file_name.split(".")[0] + "_err" + ".xls")

                # Log duplicate records
                rows = arcpy.SearchCursor(config.DuplicateRows)

                row_count = 1
                col_count = 0
                row = None
                for row in rows:
                    message = ['Duplicate record in the BCA report', row.Project_Ref_No, row.Project_Title,
                               row.House_Blk_No, row.Road_Name, row.Level_No, row.Unit_No, row.Building_Name,
                               row.Postal_Code, row.Project_Mukim_nos, row.Project_Lot_nos, row.Permit_Type_of_Work,
                               row.Type_of_Work, row.Owner_s_name, row.Owner_s_firm_name, row.Owner_s_address,
                               row.Owner_s_Tel_No, row.Owner_s_Email_address, row.Builder_s_name,
                               row.Builder_s_firm_name, row.Builder_s_address, row.Builder_s_Tel_No,
                               row.Builder_s_email_address, row.PE_s_name, row.PE_s_firm_name, row.PE_s_address,
                               row.PE_s_Tel_No, row.PE_s_Email_address, row.Architect_s_name, row.Architect_s_firm_name,
                               row.Architect_s_address, row.Architect_s_Tel_No, row.Architect_s_Email_address,
                               row.Project_Cost, row.Project_Duration, row.Approval_Date_DD_MM_YYYY_]
                    col_count = 0
                    for element in message:
                        sheet.write(row_count, col_count, element)
                        col_count += 1
                    row_count += 1
                wbk.save(config.ErrorLogFolder + "\\" + input_file_name.split(".")[0] + "_err" + ".xls")
                if row:
                    del row
                if rows:
                    del rows
            except:
                log_error("Error in 09 Log duplicate records: ", logFile)

            logFile.writelines("09 Log duplicate records ends at " + str(datetime.datetime.now()) + "\n")

            # 10. Split rows based on Mukim numbers
            logFile.writelines("10 Splitting of rows based on mukim starts at " + str(datetime.datetime.now()) + "\n")

            try:
                if arcpy.Exists(config.SplittedMukimRows):
                    arcpy.Delete_management(config.SplittedMukimRows)
                    arcpy.CreateTable_management(config.TempDataGDB, "Splitted_rows_mukim_03", config.TemplateBCAReport, "")
                else:
                    arcpy.CreateTable_management(config.TempDataGDB, "Splitted_rows_mukim_03", config.TemplateBCAReport, "")

                if arcpy.Exists(config.SplittedProjLotRows):
                    arcpy.Delete_management(config.SplittedProjLotRows)
                    arcpy.CreateTable_management(config.TempDataGDB, "Splitted_rows_projlot_04", config.TemplateBCAReport, "")
                else:
                    arcpy.CreateTable_management(config.TempDataGDB, "Splitted_rows_projlot_04", config.TemplateBCAReport, "")

                rows_in = arcpy.SearchCursor(config.UniqueRows)
                rows_out = arcpy.InsertCursor(config.SplittedMukimRows)

                for row in rows_in:
                    list_mukim_nos = row.Project_Mukim_nos.split(",")
                    for proj_mukim_nos_id in list_mukim_nos:
                        new_row_out = rows_out.newRow()
                        new_row_out.Project_Mukim_nos = proj_mukim_nos_id
                        new_row_out.PROJECTMUKIM_RAW = row.Project_Mukim_nos
                        new_row_out.Project_Ref_No = row.Project_Ref_No
                        new_row_out.Project_Title = row.Project_Title
                        new_row_out.House_Blk_No = row.House_Blk_No
                        new_row_out.Road_Name = row.Road_Name
                        new_row_out.Level_No = row.Level_No
                        new_row_out.Unit_No = row.Unit_No
                        new_row_out.Building_Name = row.Building_Name
                        new_row_out.Postal_Code = row.Postal_Code
                        new_row_out.Project_Lot_nos = row.Project_Lot_nos
                        new_row_out.Permit_Type_of_Work = row.Permit_Type_of_Work
                        new_row_out.Type_of_Work = row.Type_of_Work
                        new_row_out.Owner_s_name = row.Owner_s_name
                        new_row_out.Owner_s_firm_name = row.Owner_s_firm_name
                        new_row_out.Owner_s_address = row.Owner_s_address
                        new_row_out.Owner_s_Tel_No = row.Owner_s_Tel_No
                        new_row_out.Owner_s_Email_address = row.Owner_s_Email_address
                        new_row_out.Builder_s_name = row.Builder_s_name
                        new_row_out.Builder_s_firm_name = row.Builder_s_firm_name
                        new_row_out.Builder_s_address = row.Builder_s_address
                        new_row_out.Builder_s_Tel_No = row.Builder_s_Tel_No
                        new_row_out.Builder_s_email_address = row.Builder_s_email_address
                        new_row_out.PE_s_name = row.PE_s_name
                        new_row_out.PE_s_firm_name = row.PE_s_firm_name
                        new_row_out.PE_s_address = row.PE_s_address
                        new_row_out.PE_s_Tel_No = row.PE_s_Tel_No
                        new_row_out.PE_s_Email_address = row.PE_s_Email_address
                        new_row_out.Architect_s_name = row.Architect_s_name
                        new_row_out.Architect_s_firm_name = row.Architect_s_firm_name
                        new_row_out.Architect_s_address = row.Architect_s_address
                        new_row_out.Architect_s_Tel_No = row.Architect_s_Tel_No
                        new_row_out.Architect_s_Email_address = row.Architect_s_Email_address
                        new_row_out.Project_Cost = row.Project_Cost
                        new_row_out.Project_Duration = row.Project_Duration
                        new_row_out.Approval_Date_DD_MM_YYYY_ = row.Approval_Date_DD_MM_YYYY_
                        rows_out.insertRow(new_row_out)
                if row:
                    del row
                if new_row_out:
                    del new_row_out
                if rows_in:
                    del rows_in
                if rows_out:
                    del rows_out
            except:
                log_error("Error in 10 Splitting of rows based on mukim: ", logFile)

            logFile.writelines("10 Splitting of rows based on mukim ends at " + str(datetime.datetime.now()) + "\n")

            # 11.Split rows based on Project lot numbers
            arcpy.AddMessage("Splitting rows based on project lots")

            logFile.writelines(
                "11 Splitting of rows based on project lot starts at " + str(datetime.datetime.now()) + "\n")

            try:
                rows_in03 = arcpy.SearchCursor(config.SplittedMukimRows)
                rows_out04 = arcpy.InsertCursor(config.SplittedProjLotRows)

                for row in rows_in03:
                    list_proj_lot_nos = row.Project_Lot_nos.split(",")
                    print list_proj_lot_nos
                    for proj_lot_nos_id in list_proj_lot_nos:
                        print proj_lot_nos_id
                        new_row_out = rows_out04.newRow()
                        new_row_out.Project_Lot_nos = proj_lot_nos_id
                        new_row_out.PROJECTMUKIM_RAW = row.PROJECTMUKIM_RAW
                        new_row_out.PROJECTLOT_RAW = row.Project_Lot_nos
                        new_row_out.Project_Ref_No = row.Project_Ref_No
                        new_row_out.Project_Title = row.Project_Title
                        new_row_out.House_Blk_No = row.House_Blk_No
                        new_row_out.Road_Name = row.Road_Name
                        new_row_out.Level_No = row.Level_No
                        new_row_out.Unit_No = row.Unit_No
                        new_row_out.Building_Name = row.Building_Name
                        new_row_out.Postal_Code = row.Postal_Code
                        new_row_out.Project_Mukim_nos = row.Project_Mukim_nos
                        new_row_out.Permit_Type_of_Work = row.Permit_Type_of_Work
                        new_row_out.Type_of_Work = row.Type_of_Work
                        new_row_out.Owner_s_name = row.Owner_s_name
                        new_row_out.Owner_s_firm_name = row.Owner_s_firm_name
                        new_row_out.Owner_s_address = row.Owner_s_address
                        new_row_out.Owner_s_Tel_No = row.Owner_s_Tel_No
                        new_row_out.Owner_s_Email_address = row.Owner_s_Email_address
                        new_row_out.Builder_s_name = row.Builder_s_name
                        new_row_out.Builder_s_firm_name = row.Builder_s_firm_name
                        new_row_out.Builder_s_address = row.Builder_s_address
                        new_row_out.Builder_s_Tel_No = row.Builder_s_Tel_No
                        new_row_out.Builder_s_email_address = row.Builder_s_email_address
                        new_row_out.PE_s_name = row.PE_s_name
                        new_row_out.PE_s_firm_name = row.PE_s_firm_name
                        new_row_out.PE_s_address = row.PE_s_address
                        new_row_out.PE_s_Tel_No = row.PE_s_Tel_No
                        new_row_out.PE_s_Email_address = row.PE_s_Email_address
                        new_row_out.Architect_s_name = row.Architect_s_name
                        new_row_out.Architect_s_firm_name = row.Architect_s_firm_name
                        new_row_out.Architect_s_address = row.Architect_s_address
                        new_row_out.Architect_s_Tel_No = row.Architect_s_Tel_No
                        new_row_out.Architect_s_Email_address = row.Architect_s_Email_address
                        new_row_out.Project_Cost = row.Project_Cost
                        new_row_out.Project_Duration = row.Project_Duration
                        new_row_out.Approval_Date_DD_MM_YYYY_ = row.Approval_Date_DD_MM_YYYY_
                        rows_out04.insertRow(new_row_out)

                if row:
                    del row
                if new_row_out:
                    del new_row_out
                if rows_in03:
                    del rows_in03
                if rows_out04:
                    del rows_out04
                # print int(arcpy.GetCount_management(SplittedProjLotRows).getOutput(0))
            except:
                log_error("Error in 11 Splitting of rows based on project lot: ", logFile)
            logFile.writelines(
                "11 Splitting of rows based on project lot ends at " + str(datetime.datetime.now()) + "\n")

            # 12. Remove spaces in Mukim and Project lot values
            logFile.writelines(
                "12 Removing of spaces in mukim and project lot starts at " + str(datetime.datetime.now()) + "\n")
            arcpy.AddMessage("Cleaning project lots")
            try:

                rowsSpaces = arcpy.UpdateCursor(config.SplittedProjLotRows)

                for row in rowsSpaces:
                    lot_no_spaces = row.Project_Lot_nos.strip()
                    mukim_no_spaces = row.Project_Mukim_nos.strip()
                    row.Project_Lot_nos = lot_no_spaces
                    row.Project_Mukim_nos = mukim_no_spaces
                    rowsSpaces.updateRow(row)
                if row:
                    del row
                if rowsSpaces:
                    del rowsSpaces
            except:
                log_error("Error in 12 Removing of spaces in mukim and project lot: ", logFile)
            logFile.writelines(
                "12 Removing of spaces in mukim and project lot ends at " + str(datetime.datetime.now()) + "\n")

            # 13. Log empty Mukimlot or date fields
            logFile.writelines(
                "13 Log empty mukim and project lot nos  starts at " + str(datetime.datetime.now()) + "\n")

            try:
                rowsEmpty = arcpy.UpdateCursor(config.SplittedProjLotRows)

                for row in rowsEmpty:
                    message = ['Missing Project lot or Mukim numbers', row.Project_Ref_No, row.Project_Title,
                               row.House_Blk_No, row.Road_Name, row.Level_No, row.Unit_No, row.Building_Name,
                               row.Postal_Code, row.Project_Mukim_nos, row.Project_Lot_nos, row.Permit_Type_of_Work,
                               row.Type_of_Work, row.Owner_s_name, row.Owner_s_firm_name, row.Owner_s_address,
                               row.Owner_s_Tel_No, row.Owner_s_Email_address, row.Builder_s_name,
                               row.Builder_s_firm_name, row.Builder_s_address, row.Builder_s_Tel_No,
                               row.Builder_s_email_address, row.PE_s_name, row.PE_s_firm_name, row.PE_s_address,
                               row.PE_s_Tel_No, row.PE_s_Email_address, row.Architect_s_name, row.Architect_s_firm_name,
                               row.Architect_s_address, row.Architect_s_Tel_No, row.Architect_s_Email_address,
                               row.Project_Cost, row.Project_Duration, row.Approval_Date_DD_MM_YYYY_]
                    message2 = ['Missing Project duration or Approval date', row.Project_Ref_No, row.Project_Title,
                                row.House_Blk_No, row.Road_Name, row.Level_No, row.Unit_No, row.Building_Name,
                                row.Postal_Code, row.Project_Mukim_nos, row.Project_Lot_nos, row.Permit_Type_of_Work,
                                row.Type_of_Work, row.Owner_s_name, row.Owner_s_firm_name, row.Owner_s_address,
                                row.Owner_s_Tel_No, row.Owner_s_Email_address, row.Builder_s_name,
                                row.Builder_s_firm_name, row.Builder_s_address, row.Builder_s_Tel_No,
                                row.Builder_s_email_address, row.PE_s_name, row.PE_s_firm_name, row.PE_s_address,
                                row.PE_s_Tel_No, row.PE_s_Email_address, row.Architect_s_name,
                                row.Architect_s_firm_name, row.Architect_s_address, row.Architect_s_Tel_No,
                                row.Architect_s_Email_address, row.Project_Cost, row.Project_Duration,
                                row.Approval_Date_DD_MM_YYYY_]
                    if row.Project_Mukim_nos is None or (len(row.Project_Mukim_nos) < 4):
                        col_count = 0
                        for element in message:
                            sheet.write(row_count, col_count, element)
                            col_count += 1
                        row_count += 1
                        rowsEmpty.deleteRow(row)
                    elif row.Project_Lot_nos is None or (len(row.Project_Lot_nos) == 0):
                        col_count = 0
                        for element in message:
                            sheet.write(row_count, col_count, element)
                            col_count += 1
                        row_count += 1
                        rowsEmpty.deleteRow(row)
                    if row.Project_Duration is None or (len(row.Project_Duration) < 1):
                        col_count = 0
                        for element in message2:
                            sheet.write(row_count, col_count, element)
                            col_count += 1
                        row_count += 1
                        rowsEmpty.deleteRow(row)

                    elif row.Approval_Date_DD_MM_YYYY_ is None or (len(row.Approval_Date_DD_MM_YYYY_) < 1):
                        col_count = 0
                        for element in message2:
                            sheet.write(row_count, col_count, element)
                            col_count += 1
                        row_count += 1
                        rowsEmpty.deleteRow(row)
                wbk.save(config.ErrorLogFolder + "\\" + input_file_name.split(".")[0] + "_err" + ".xls")
                if row:
                    del row
                if rowsEmpty:
                    del rowsEmpty
            except:
                log_error("Error in 13 Log for empty mukim and project lot nos: ", logFile)
            logFile.writelines("13 Log empty mukim and project lot nos ends at " + str(datetime.datetime.now()) + "\n")

            # 14. Error log for those with bad values
            arcpy.AddMessage("14 Logging bad values")
            logFile.writelines("14 Log if bad values exist starts at " + str(datetime.datetime.now()) + "\n")
            try:
                rowsBadValues = arcpy.UpdateCursor(config.SplittedProjLotRows)

                for row in rowsBadValues:
                    message = ['Mukim or Project lot numbers have bad values', row.Project_Ref_No, row.Project_Title,
                               row.House_Blk_No, row.Road_Name, row.Level_No, row.Unit_No, row.Building_Name,
                               row.Postal_Code, row.Project_Mukim_nos, row.Project_Lot_nos, row.Permit_Type_of_Work,
                               row.Type_of_Work, row.Owner_s_name, row.Owner_s_firm_name, row.Owner_s_address,
                               row.Owner_s_Tel_No, row.Owner_s_Email_address, row.Builder_s_name,
                               row.Builder_s_firm_name, row.Builder_s_address, row.Builder_s_Tel_No,
                               row.Builder_s_email_address, row.PE_s_name, row.PE_s_firm_name, row.PE_s_address,
                               row.PE_s_Tel_No, row.PE_s_Email_address, row.Architect_s_name, row.Architect_s_firm_name,
                               row.Architect_s_address, row.Architect_s_Tel_No, row.Architect_s_Email_address,
                               row.Project_Cost, row.Project_Duration, row.Approval_Date_DD_MM_YYYY_]
                    if len(REGEX_FOR_INVALID_CHARS.findall(row.Project_Lot_nos)) > 0:
                        col_count = 0
                        for element in message:
                            sheet.write(row_count, col_count, element)
                            col_count += 1
                        row_count += 1
                        rowsBadValues.deleteRow(row)
                    elif len(REGEX_FOR_INVALID_CHARS.findall(row.Project_Mukim_nos)) > 0:
                        col_count = 0
                        for element in message:
                            sheet.write(row_count, col_count, element)
                            col_count += 1
                        row_count += 1
                        rowsBadValues.deleteRow(row)
                    elif len(uptodigit(row.Project_Lot_nos)) > 0:
                        col_count = 0
                        for element in message:
                            sheet.write(row_count, col_count, element)
                            col_count += 1
                        row_count += 1
                        rowsBadValues.deleteRow(row)
                wbk.save(config.ErrorLogFolder + "\\" + input_file_name.split(".")[0] + "_err" + ".xls")

                if row:
                    del row
                if rowsBadValues:
                    del rowsBadValues
            except:
                log_error("Error in 14 Log if bad values exist: ", logFile)
            logFile.writelines("14 Log if bad values exist ends at " + str(datetime.datetime.now()) + "\n")

            # 15. Add zeros for Project Lot numbers
            logFile.writelines("15 Add zeros starts at " + str(datetime.datetime.now()) + "\n")
            try:
                rowsZeros = arcpy.UpdateCursor(config.SplittedProjLotRows)
                letters = string.ascii_letters
                for row in rowsZeros:
                    letter_count = len(filter(functools.partial(operator.contains, letters), row.Project_Lot_nos))
                    filled_string = row.Project_Lot_nos.zfill(5 + letter_count)
                    row.Project_Lot_nos = filled_string
                    rowsZeros.updateRow(row)
                if row:
                    del row
                if rowsZeros:
                    del rowsZeros
            except:
                log_error("Error in 15 Add zeros: ", logFile)
            logFile.writelines("15 Add zeros ends at " + str(datetime.datetime.now()) + "\n")

            # 16. Add and populate fields Mukim_Lot_No, Mukimlot_wo_letter, and Permit_date
            logFile.writelines("16 Add and populate fields starts at " + str(datetime.datetime.now()) + "\n")
            try:
                rowsPop = arcpy.UpdateCursor(config.SplittedProjLotRows)
                for row in rowsPop:
                    expression = str(row.Project_Mukim_nos) + "-" + str(row.Project_Lot_nos)
                    row.Mukim_Lot_No = expression
                    date = filedate.strftime("%Y%m%d")
                    year = int(date[:4])
                    month = int(date[4:6])
                    day = int(date[6:8])
                    permit_date = datetime.datetime(year, month, day)
                    row.Permit_date = permit_date
                    rowsPop.updateRow(row)
                if row:
                    del row
                if rowsPop:
                    del rowsPop
                # Calculate Mukimlot_wo_letter
                arcpy.CalculateField_management(config.SplittedProjLotRows, "Mukimlot_wo_letter", "!Mukim_Lot_No![:10]",
                                                "PYTHON_9.3", "")

            except:
                log_error("Error in 16 Add and populate fields: ", logFile)
            logFile.writelines("16 Add and populate fields ends at " + str(datetime.datetime.now()) + "\n")

            # 17.Match mukim lot and land lot
            logFile.writelines("17 Match mukim lot with landlot starts at " + str(datetime.datetime.now()) + "\n")
            try:
                arcpy.MatchMukimLandLot()
            except:
                log_error("Error in 17 Match mukim lot with landlot: ", logFile)
            logFile.writelines("17 Match mukim lot with landlot ends at " + str(datetime.datetime.now()) + "\n")

            # 18.Get unmatched mukim lot with land lot
            logFile.writelines("18 Get unmatched mukim lot starts at " + str(datetime.datetime.now()) + "\n")
            arcpy.AddMessage("18 Get unmatched mukim lot")
            try:
                arcpy.GetUnmatchedMukimLot()

            except:
                log_error("Error in 18 Get unmatched mukim lot: ", logFile)

            logFile.writelines("18 Get unmatched mukim lot ends at " + str(datetime.datetime.now()) + "\n")

            # 19. Log errors for unmatched mukim lots
            logFile.writelines("19 Log unmatched mukim lot starts at " + str(datetime.datetime.now()) + "\n")
            try:
                rowsUnmatched = arcpy.SearchCursor(config.UnmatchedMukimLot)
                row = None

                for row in rowsUnmatched:
                    message = ['Unmatched mukim lot with the land lot', row.Project_Ref_No, row.Project_Title,
                               row.House_Blk_No, row.Road_Name, row.Level_No, row.Unit_No, row.Building_Name,
                               row.Postal_Code, row.Project_Mukim_nos, row.Project_Lot_nos, row.Permit_Type_of_Work,
                               row.Type_of_Work, row.Owner_s_name, row.Owner_s_firm_name, row.Owner_s_address,
                               row.Owner_s_Tel_No, row.Owner_s_Email_address, row.Builder_s_name,
                               row.Builder_s_firm_name, row.Builder_s_address, row.Builder_s_Tel_No,
                               row.Builder_s_email_address, row.PE_s_name, row.PE_s_firm_name, row.PE_s_address,
                               row.PE_s_Tel_No, row.PE_s_Email_address, row.Architect_s_name, row.Architect_s_firm_name,
                               row.Architect_s_address, row.Architect_s_Tel_No, row.Architect_s_Email_address,
                               row.Project_Cost, row.Project_Duration, row.Approval_Date_DD_MM_YYYY_]
                    col_count = 0
                    for element in message:
                        sheet.write(row_count, col_count, element)
                        col_count += 1
                    row_count += 1
                wbk.save(config.ErrorLogFolder + "\\" + input_file_name.split(".")[0] + "_err" + ".xls")
                if row:
                    del row
                if rowsUnmatched:
                    del rowsUnmatched

                with xlrd.open_workbook(config.ErrorLogFolder + "\\" + input_file_name.split(".")[0] + "_err" + ".xls") as wb:
                    sh = wb.sheet_by_index(0)
                    if sh.nrows == 1:
                        os.remove(config.ErrorLogFolder + "\\" + input_file_name.split(".")[0] + "_err" + ".xls")

            except arcpy.ExecuteError:
                log_error("Error in 19 Log unmatched mukim lot: ", logFile)
            logFile.writelines("19 Log unmatched mukim lot ends at " + str(datetime.datetime.now()) + "\n")

            # 20. Prepare the table for MukimConstruct matching (add required fields)
            logFile.writelines("20 Add fields to be used for matching starts at " + str(datetime.datetime.now()) + "\n")
            try:
                if arcpy.Exists(config.MUKIMCONSTRUCTImport):
                    arcpy.Delete_management(config.MUKIMCONSTRUCTImport)
                    arcpy.FeatureClassToFeatureClass_conversion(config.MukimConstructSource, config.TempDataGDB,
                                                                "MUKIM_CONSTRUCT_Import")
                else:
                    arcpy.FeatureClassToFeatureClass_conversion(config.MukimConstructSource, config.TempDataGDB,
                                                                "MUKIM_CONSTRUCT_Import")

                arcpy.AddField_management(config.MatchedMukimLot, "Concat_4fields", "Text", "", "", "")
                arcpy.AddField_management(config.MUKIMCONSTRUCTImport, "Concat_4fields", "Text", "", "", "")
                arcpy.AddField_management(config.MatchedMukimLot, "PROJ_DURATION_MTHS2", "Double", "", "", "")
            except:
                log_error("Error in 20 Add fields to be used for matching: ", logFile)
            logFile.writelines("20 Add fields to be used for matching ends at " + str(datetime.datetime.now()) + "\n")

            # 21. Calculate Project Duration as months
            logFile.writelines("21 Calculate PROJ_DURATION as months starts at " + str(datetime.datetime.now()) + "\n")
            try:
                rowsProjDur = arcpy.UpdateCursor(config.MatchedMukimLot)

                for row in rowsProjDur:
                    durationstr = row.PROJ_DURATION_MTHS
                    if "Month" in row.PROJ_DURATION_MTHS:
                        durationintmth = int(durationstr.split(' ')[0])
                        row.PROJ_DURATION_MTHS2 = durationintmth
                    elif "Year" in row.PROJ_DURATION_MTHS:
                        durationintyr = int(durationstr.split(' ')[0]) * 12
                        row.PROJ_DURATION_MTHS2 = durationintyr
                    rowsProjDur.updateRow(row)
                if rowsProjDur:
                    del rowsProjDur
                if row:
                    del row

                arcpy.DeleteField_management(config.MatchedMukimLot, "PROJ_DURATION_MTHS")
                arcpy.AddField_management(config.MatchedMukimLot, "PROJ_DURATION_MTHS", "Double")
                arcpy.CalculateField_management(config.MatchedMukimLot, "PROJ_DURATION_MTHS", "[PROJ_DURATION_MTHS2]")
            except:
                log_error("Error in 21 Calculate PROJ_DURATION as months: ", logFile)
            logFile.writelines("21 Calculate PROJ_DURATION as months ends at " + str(datetime.datetime.now()) + "\n")

            # 22. Concatenate 4 fields to be used in checking if mukimlot already exists in MUKIMCONSTRUCT
            logFile.writelines("22 Concatenate 4 fields starts at " + str(datetime.datetime.now()) + "\n")
            try:
                rowsConcat1 = arcpy.UpdateCursor(config.MUKIMCONSTRUCTImport)

                for row in rowsConcat1:
                    expression = str(row.PROJ_REF_NO) + "-" + str(row.BUILDER_NAME) + "-" + str(
                        row.LOT_KEY) + "-" + str(row.PERMIT_DATE)
                    row.Concat_4fields = expression
                    rowsConcat1.updateRow(row)
                if row:
                    del row
                if rowsConcat1:
                    del rowsConcat1

                rowsConcat2 = arcpy.UpdateCursor(config.MatchedMukimLot)

                for row in rowsConcat2:
                    expression = str(row.PROJ_REF_NO) + "-" + str(row.BUILDER_NAME) + "-" + str(
                        row.LOT_KEY) + "-" + str(row.PERMIT_DATE)
                    row.Concat_4fields = expression
                    rowsConcat2.updateRow(row)
                if row:
                    del row
                if rowsConcat2:
                    del rowsConcat2
            except:
                log_error("Error in 22 Concatenate 4 fields: ", logFile)
            logFile.writelines("22 Concatenate 4 fields ends at " + str(datetime.datetime.now()) + "\n")

            # 23.Match mukim lot with mukim construct
            logFile.writelines("23 Match mukimlot with mukim construct at " + str(datetime.datetime.now()) + "\n")
            arcpy.env.workspace = config.TempDataGDB # "G:\\Project\\GERIUPGRADE\\GPTools\\NotificationSysTools\\BCAReportProcessing\\Temp_data.gdb"
            try:
                arcpy.MatchedMukimlotMukimConstruct()
            except:
                log_error("Error in 23 Match mukimlot with mukim construct: ", logFile)
            logFile.writelines("23 Match mukimlot with mukim construct ends at " + str(datetime.datetime.now()) + "\n")

            # 24.Copy raw values to project lot and project mukim columns and delete the 2 fields
            logFile.writelines("24 Recalculate projlot and projmukim based on original values starts at " + str(
                datetime.datetime.now()) + "\n")
            try:
                rowsRaw = arcpy.UpdateCursor(config.MatchedMukimLot)

                for row in rowsRaw:
                    row.PROJ_MUKIM_NOS = row.PROJECTMUKIM_RAW
                    row.PROJ_LOT_NOS = row.PROJECTLOT_RAW
                    rowsRaw.updateRow(row)
                if row:
                    del row
                if rowsRaw:
                    del rowsRaw
            except:
                log_error("Error in 24 Recalculate projlot and projmukim based on original values:", logFile)
            logFile.writelines("24 Recalculate projlot and projmukim based on original values ends at " + str(
                datetime.datetime.now()) + "\n")

            # 25. Export Cleaned BCA Permit report for CWD
            logFile.writelines(
                "25 Export of Cleaned BCA Permit report starts at " + str(datetime.datetime.now()) + "\n")
            try:
                # Initialize the file
                CleanedBCAPermitReport = xlwt.Workbook()
                book = CleanedBCAPermitReport.add_sheet('Book 1')
                countrow = 0
                countcol = 0
                fields = ['Project Ref No', 'Project Title', 'House Blk No', 'Road Name', 'Level No', 'Unit No',
                          'Building Name', 'Postal Code', 'Project Mukim nos', 'Project Lot nos', 'Permit Type of Work',
                          'Type of Work', "Owner's name", "Owner's firm name", "Owner's address", "Owner's Tel No",
                          "Owner's Email address", "Builder's name", "Builder's firm name", "Builder's address",
                          "Builder's Tel No", "Builder's email address", "PE's name", "PE's firm name", "PE's address",
                          "PE's Tel No", "PE's Email address", "Architect's name", "Architect's firm name",
                          "Architect's address", "Architect's Tel No", "Architect's Email address", 'Project Cost',
                          'Project Duration', 'Approval Date(DD/MM/YYYY)']
                for fieldname in fields:
                    book.write(countrow, countcol, fieldname)
                    countcol += 1
                CleanedBCAPermitReport.save(config.CleanedBCAPermitFolder + "\\" + input_file_name.split(".")[0] + ".xls")

                # Copy the data to Excel File
                data = arcpy.SearchCursor(config.MatchedMukimLot)

                countrow = 1
                countcol = 0
                for row in data:
                    message = [row.PROJ_REF_NO, row.PROJ_TITLE, row.HOUSE_BLK_NO, row.ROAD_NAME, row.LEVEL_NO,
                               row.UNIT_NO, row.BUILDING_NAME, row.POSTAL_CODE, row.PROJ_MUKIM_NOS, row.PROJ_LOT_NOS,
                               row.PERMIT_WORK_TYPE, row.WORK_TYPE, row.OWNER_NAME, row.OWNER_FIRM_NAME, row.OWNER_ADDR,
                               row.OWNER_TEL, row.OWNER_EMAIL, row.BUILDER_NAME, row.BUILDER_FIRM_NAME,
                               row.BUILDER_ADDR, row.BUILDER_TEL, row.BUILDER_EMAIL, row.PE_NAME, row.PE_FIRM_NAME,
                               row.PE_ADDR, row.PE_TEL, row.PE_EMAIL, row.ARCHITECT_NAME, row.ARCHITECT_FIRM_NAME,
                               row.ARCHITECT_ADDR, row.ARCHITECT_TEL, row.ARCHITECT_EMAIL, row.PROJ_COST,
                               row.PROJ_DURATION_MTHS, row.PROJ_APPROVAL_DATE]
                    countcol = 0
                    for element in message:
                        book.write(countrow, countcol, element)
                        countcol += 1
                    countrow += 1
                CleanedBCAPermitReport.save(config.CleanedBCAPermitFolder + "\\" + input_file_name.split(".")[0] + ".xls")
                if row:
                    del row
                if data:
                    del data
            except:
                log_error("Error in 25 Export of Cleaned BCA Permit Report: Error in 26 Catchment calculation: ", logFile)
            logFile.writelines("25 Export of Cleaned BCA Permit Report ends at " + str(datetime.datetime.now()) + "\n")

            # 26. Catchment calculation
            arcpy.env.workspace = config.TempDataGDB
            logFile.writelines("26 Catchment calculation starts at " + str(datetime.datetime.now()) + "\n")
            try:
                arcpy.CatchmentCalculation()
            except:
                log_error("Error in 26 Catchment calculation: ", logFile)
            logFile.writelines("26 Catchment calculation ends at " + str(datetime.datetime.now()) + "\n")

            # 27. Depot calculation
            logFile.writelines("27 Depot calculation starts at " + str(datetime.datetime.now()) + "\n")
            try:
                arcpy.DepotCalculation()
            except:
                log_error("Error in 27 Depot calculation: ", logFile)
            logFile.writelines("27 Depot calculation ends at " + str(datetime.datetime.now()) + "\n")

            # 28. Re-add date fields and populate
            logFile.writelines("28 Re-add date fields  and populate starts at " + str(datetime.datetime.now()) + "\n")
            try:
                arcpy.AddField_management(config.MUKIMCONSTRUCT_Temp, "PERMIT_DATE", "Date")
                arcpy.AddField_management(config.MUKIMCONSTRUCT_Temp, "PROJ_APPROVAL_DATE2", "Date")
                arcpy.AddField_management(config.MUKIMCONSTRUCT_Temp, "PROJ_END_DATE", "Date")

                rows = arcpy.UpdateCursor(config.MUKIMCONSTRUCT_Temp)

                for row in rows:
                    date = filedate.strftime("%Y%m%d")
                    year = int(date[:4])
                    month = int(date[4:6])
                    day = int(date[6:8])
                    permit_date = datetime.datetime(year, month, day)
                    row.PERMIT_DATE = permit_date
                    row.PROJ_APPROVAL_DATE2 = datetime.datetime.strptime(row.PROJ_APPROVAL_DATE, '%d/%m/%Y')
                    rows.updateRow(row)
                if row:
                    del row
                if rows:
                    del rows
            except:
                log_error("Error in 28 Re-add fields  and populate: ", logFile)
            logFile.writelines("28 Re-add fields  and populate ends at " + str(datetime.datetime.now()) + "\n")

            # 29. Calculate the end date field
            logFile.writelines("29 Calculate the end date field starts at " + str(datetime.datetime.now()) + "\n")
            try:

                rowsEndDate = arcpy.UpdateCursor(config.MUKIMCONSTRUCT_Temp)

                for row in rowsEndDate:
                    sourcedate = row.PROJ_APPROVAL_DATE2
                    # sourcedate = datetime.datetime.strptime(row.PROJ_APPROVAL_DATE2 , '%d/%m/%Y')
                    months = int(row.PROJ_DURATION_MTHS)
                    d = add_months(sourcedate, months)
                    row.PROJ_END_DATE = d
                    rowsEndDate.updateRow(row)
                if row:
                    del row
                if rowsEndDate:
                    del rowsEndDate
            except:
                log_error("Error in 29 Calculate the end date field: ", logFile)
            logFile.writelines("29 Calculate the end date field ends at " + str(datetime.datetime.now()) + "\n")

            # 30. Calculate Project Total Area
            logFile.writelines("30 Project total area calculation starts at " + str(datetime.datetime.now()) + "\n")
            try:
                arcpy.ProjectTotalArea()
            except:
                log_error("Error in 30 Project total area calculation: ", logFile)
            logFile.writelines("30 Project total area calculation ends at " + str(datetime.datetime.now()) + "\n")

            # 31. Calculate the BCA_CORRECTED_BY
            logFile.writelines("31 Calculate the BCA_CORRECTED_BY starts at " + str(datetime.datetime.now()) + "\n")
            try:
                rows_BCA_CB = arcpy.UpdateCursor(config.MUKIMCONSTRUCT_Temp)

                for row in rows_BCA_CB:
                    if "\WSN\\" in BCAreport:
                        row.BCA_CORRECTED_BY = "WSN"
                    elif "\WRN\\" in BCAreport:
                        row.BCA_CORRECTED_BY = "WRN"
                    elif "\CWD\\" in BCAreport:
                        row.BCA_CORRECTED_BY = "CWD"
                    rows_BCA_CB.updateRow(row)
                if row:
                    del row
                if rows_BCA_CB:
                    del rows_BCA_CB
            except:
                log_error("Error in 31 Calculate the BCA_CORRECTED_BY: ", logFile)

            # 32. Remove spaces in PROJ_REF_NO
            logFile.writelines(
                "32 Removing of spaces in mukim and project lot starts at " + str(datetime.datetime.now()) + "\n")

            try:
                rowsSpaces = arcpy.UpdateCursor(config.MUKIMCONSTRUCT_Temp)

                for row in rowsSpaces:
                    lot_no_spaces = row.PROJ_REF_NO.strip()
                    row.PROJ_REF_NO = lot_no_spaces
                    rowsSpaces.updateRow(row)
                if row:
                    del row
                if rowsSpaces:
                    del rowsSpaces
            except:
                log_error("Error in 32 Removing of spaces in mukim and project lot: ", logFile)
            logFile.writelines(
                "32 Removing of spaces in mukim and project lot ends at " + str(datetime.datetime.now()) + "\n")

            # 33. Process the Mukim Construct by Project
            logFile.writelines(
                "33 Process the Mukim Construct by Project starts at " + str(datetime.datetime.now()) + "\n")
            arcpy.env.overwriteOutput = True
            try:
                MUKIM_CONSTRUCT_BYPROJ_IMPORT = config.TempDataGDB + "\\MUKIM_CONSTRUCT_BYPROJ_IMPORT"
                MUKIMCONBYPROJ_SORT = config.TempDataGDB + "\\MUKIMCONBYPROJ_SORT"
                MUKIM_CONSTRUCT_BYPROJ_DISS = config.TempDataGDB + "\\MUKIM_CONSTRUCT_BYPROJ_DISS"
                MUKIM_CONSTRUCT_BYPROJ_DISS__2_ = config.TempDataGDB + "\\MUKIM_CONSTRUCT_BYPROJ_DISS"

                if arcpy.Exists(MUKIM_CONSTRUCT_BYPROJ_IMPORT):
                    arcpy.Delete_management(MUKIM_CONSTRUCT_BYPROJ_IMPORT)
                if arcpy.Exists(MUKIMCONBYPROJ_SORT):
                    arcpy.Delete_management(MUKIMCONBYPROJ_SORT)
                if arcpy.Exists(MUKIM_CONSTRUCT_BYPROJ_DISS):
                    arcpy.Delete_management(MUKIM_CONSTRUCT_BYPROJ_DISS)

                arcpy.MUKIMCONBYPROJ()
                # arcpy.MUKIMCONSTRUCTBYPROJProcess2()

                arcpy.Sort_management(MUKIM_CONSTRUCT_BYPROJ_IMPORT, MUKIMCONBYPROJ_SORT, "PROJ_END_DATE DESCENDING",
                                      "UR")
                arcpy.Dissolve_management(MUKIMCONBYPROJ_SORT, MUKIM_CONSTRUCT_BYPROJ_DISS, "PROJ_REF_NO",
                                          "LOT_KEY FIRST;PROJ_REF_NO FIRST;PROJ_TITLE FIRST;HOUSE_BLK_NO FIRST;ROAD_NAME FIRST;POSTAL_CODE FIRST;LEVEL_NO FIRST;UNIT_NO FIRST;BUILDING_NAME FIRST;PROJ_MUKIM_NOS FIRST;PROJ_LOT_NOS FIRST;PERMIT_WORK_TYPE FIRST;WORK_TYPE FIRST;OWNER_NAME FIRST;OWNER_FIRM_NAME FIRST;OWNER_ADDR FIRST;OWNER_TEL FIRST;OWNER_EMAIL FIRST;BUILDER_NAME FIRST;BUILDER_FIRM_NAME FIRST;BUILDER_ADDR FIRST;BUILDER_TEL FIRST;BUILDER_EMAIL FIRST;PE_NAME FIRST;PE_FIRM_NAME FIRST;PE_ADDR FIRST;PE_TEL FIRST;PE_EMAIL FIRST;ARCHITECT_NAME FIRST;ARCHITECT_FIRM_NAME FIRST;ARCHITECT_ADDR FIRST;ARCHITECT_TEL FIRST;ARCHITECT_EMAIL FIRST;PROJ_TOT_AREA FIRST;PROJ_PARENT_CWDCATCHMENT FIRST;PROJ_PARENT_WSNDEPOT FIRST;PROJ_PARENT_WRPCATCHMENT FIRST;BCA_CORRECTED_BY FIRST;PROJ_DURATION_MTHS FIRST;PROJ_COST FIRST",
                                          "MULTI_PART", "DISSOLVE_LINES")
                arcpy.JoinField_management(MUKIM_CONSTRUCT_BYPROJ_DISS, "FIRST_PROJ_REF_NO", MUKIMCONBYPROJ_SORT,
                                           "PROJ_REF_NO", "PROJ_APPROVAL_DATE;PROJ_END_DATE;PERMIT_DATE")
                arcpy.CalculateField_management(MUKIM_CONSTRUCT_BYPROJ_DISS__2_, "FIRST_PROJ_TOT_AREA",
                                                "[Shape_Area]/10000", "VB", "")

            except:
                log_error("Error in 33 Process the Mukim Construct by Project: ", logFile)
            logFile.writelines(
                "33 Process the Mukim Construct by Project ends at " + str(datetime.datetime.now()) + "\n")
            arcpy.AddMessage("33 END process MUKIM CONSTRUCT")

            # 34. Filter on-going projects

            logFile.writelines("34 Filter on-going projects starts at " + str(datetime.datetime.now()) + "\n")
            try:
                # TempDataGDB = "G:\\Project\\GERIUPGRADE\\GPTools\\NotificationSysTools\\BCAReportProcessing\\Temp_data.gdb"
                MUKIM_CONSTRUCT_BYPROJ_DISS = config.TempDataGDB + "\\MUKIM_CONSTRUCT_BYPROJ_DISS"
                rowsIn = arcpy.UpdateCursor(MUKIM_CONSTRUCT_BYPROJ_DISS)

                row = None
                for row in rowsIn:
                    strdays = str(row.PROJ_END_DATE.date() - datetime.date.today())
                    splitDays = strdays.split()
                    if splitDays[0] == '0:00:00':
                        result = "On-going project (but will end today)"
                    else:
                        if int(splitDays[0]) < 0:
                            rowsIn.deleteRow(row)
                        else:
                            result = "On-going project"
                if rowsIn:
                    del rowsIn
                if row:
                    del row

            except:
                log_error("Error in 34 Filter on-going projects: ", logFile)
            logFile.writelines("34 Filter on-going projects ends at " + str(datetime.datetime.now()) + "\n")

            # 35. Append the new data to MUKIM_CONSTRUCT
            logFile.writelines(
                "35 Append the new data to MUKIM_CONSTRUCT starts at " + str(datetime.datetime.now()) + "\n")
            try:
                arcpy.AppendNewData()
            except:
                log_error("Error in 35 Append the new data to MUKIM_CONSTRUCT: ", logFile)
            logFile.writelines(
                "35 Append the new data to MUKIM_CONSTRUCT ends at " + str(datetime.datetime.now()) + "\n")

            # Clean the memory and the schema lock
            arcpy.RefreshCatalog(config.Notification)
            arcpy.Compact_management(config.TempDataGDB)
            gc.collect()

            # Status update to run/not run the SiteInspection Update
            Log_SiteInspectionUpdate = file(config.SiteInspectionUpdate, "w")
            Log_SiteInspectionUpdate.writelines("YES")
            Log_SiteInspectionUpdate.close()

            arcpy.AddMessage("END BCA Processing")
            arcpy.AddMessage("Passing file date to other functions: " + repr(filedate))

            # Generate Report
            import ReportGeneration_Adhoc_WithProjects as gen_report
            gen_report.run(filedate)
            #
            # # Send email to departments
            # import EmailGenerationCompletion_adhoc as send_dept_notification
            # if "CORRECTED" in BCAreport.upper():
            #     send_dept_notification.run(filedate, corrected=True)
            # else:
            #     send_dept_notification.run(filedate)

            # Generate advisory letters
            import LetterGeneration as letter_gen
            letter_gen.run(filedate)
            #
            # # Send letters to project team
            # import EmailGeneration as send_advisory_email
            # send_advisory_email.run(filedate)


    # 36. Move the BCAReport in the backup folder
    for BCAreport in correct_config_files:

        input_file_name = BCAreport.split("\\")[-1]
        bk_file_path = os.path.join(config.BCAreportBackupFolder, input_file_name)

        # if the same file name exists in the backup folder, rename the new file with timestamp and move
        if os.path.exists(bk_file_path):

            new_filename = datetime.datetime.now().strftime("%Y%m%d-%H%M") + input_file_name
            new_filepath = os.path.join(config.BCAreportBackupFolder, new_filename)
            shutil.copy(BCAreport, new_filepath)
            os.remove(BCAreport)

        # if the filename does not exist in the backup folder, move the file to backup
        else:
            shutil.move(BCAreport, config.BCAreportBackupFolder)

        logFile.writelines("Moved the BCA report to the backup folder at " + str(datetime.datetime.now()) + "\n")
    logFile.close()


if __name__ == '__main__':
    execute()
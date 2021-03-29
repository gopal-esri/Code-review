# -------------------------------------------------------------------------------
# Name:        Notification System
# Purpose:
#
# Author:      wxhow
#
# Created:     22/07/2016
# Copyright:   (c) wxhow 2016
# Licence:     <your licence>
# -------------------------------------------------------------------------------
import arcpy, os, string, collections, csv, traceback, logging, sys, shutil
import zipfile
from collections import defaultdict
from ProjectReference import *
from Constants import *
import datetime
from datetime import timedelta

# log variables
logfd = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Logs")
logfile = datetime.datetime.now().strftime("%Y%m%d%H%M") + "_ReportGeneration.log"

# log variables
if not os.path.isdir(logfd):
    os.makedirs(logfd)

logger = logging.getLogger(__name__)
hdlr = logging.FileHandler(os.path.join(logfd, logfile))
formatter = logging.Formatter('%(asctime)s;%(levelname)s;%(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.INFO)


def Error_Handler(message):
    global logger
    logger.error(message)
    tb = sys.exc_info()[2]
    tbinfo = traceback.format_tb(tb)[0]
    logger.error("{0} , <{1}>".format(sys.exc_info()[0], sys.exc_info()[1]))
    logger.error(tbinfo)


def check_report_exists(report_name, dt):
    """
    :param dt: date time of the permit report
    :return: True/False if the report is successfully generated
    """

    report_path = os.path.join(os.path.join(os.path.dirname(os.path.abspath(__file__)), "Reports//{}".format(dt.strftime("%Y-%m-%d"))), report_name)
    print(report_path)

    if os.path.exists(report_path):
        return True
    else:
        return False


def StartOfWeek(dt, startOfWeek):
    diff = dt.weekday() - startOfWeek
    if (diff < 0):
        diff += 7

    return dt + timedelta(days=(-1 * diff))


def QueryMukimLot(dt):
    print("Start querying mukim lot: " + repr(dt))

    # dt = StartOfWeek(datetime.today(), 6)
    # Change the date here
    dt_time = dt.strftime("%Y-%m-%d 00:00:00")
    today_str = datetime.datetime.now().strftime("%Y%m%d")

    expression = NFQuery.PERMIT.format(dt_time, today_str)
    print(expression)

    fields = [MUKIM_CONSTRUCT.BUILDER_FIRM_NAME, MUKIM_CONSTRUCT.BUILDER_NAME, MUKIM_CONSTRUCT.BUILDER_TEL,
              MUKIM_CONSTRUCT.LOT_KEY, MUKIM_CONSTRUCT.OWNER_EMAIL, MUKIM_CONSTRUCT.OWNER_FIRM_NAME,
              MUKIM_CONSTRUCT.OWNER_NAME \
        , MUKIM_CONSTRUCT.PERMIT_DATE, MUKIM_CONSTRUCT.PE_NAME, MUKIM_CONSTRUCT.PROJ_PARENT_CWDCATCHMENT,
              MUKIM_CONSTRUCT.PROJ_PARENT_WRPCATCHMENT, MUKIM_CONSTRUCT.PROJ_PARENT_WSNDEPOT,
              MUKIM_CONSTRUCT.PROJ_REF_NO \
        , MUKIM_CONSTRUCT.PROJ_TITLE, MUKIM_CONSTRUCT.PROJ_TOT_AREA, MUKIM_CONSTRUCT.PROJ_END_DATE,
              MUKIM_CONSTRUCT.HOUSE_BLK_NO, MUKIM_CONSTRUCT.ROAD_NAME, MUKIM_CONSTRUCT.BUILDING_NAME,
              MUKIM_CONSTRUCT.POSTAL_CODE \
              , MUKIM_CONSTRUCT.BUILDER_EMAIL, MUKIM_CONSTRUCT.BUILDER_ADDR \
              , MUKIM_CONSTRUCT.OWNER_ADDR, MUKIM_CONSTRUCT.OWNER_TEL \
              , MUKIM_CONSTRUCT.PE_EMAIL, MUKIM_CONSTRUCT.PE_FIRM_NAME \
              , MUKIM_CONSTRUCT.PE_ADDR, MUKIM_CONSTRUCT.PE_TEL \
              , MUKIM_CONSTRUCT.ARCHITECT_NAME, MUKIM_CONSTRUCT.ARCHITECT_EMAIL \
              , MUKIM_CONSTRUCT.ARCHITECT_FIRM_NAME, MUKIM_CONSTRUCT.ARCHITECT_ADDR \
              , MUKIM_CONSTRUCT.ARCHITECT_TEL \
              , MUKIM_CONSTRUCT.PERMIT_WORK_TYPE, MUKIM_CONSTRUCT.PROJ_COST \
              , MUKIM_CONSTRUCT.PROJ_DURATION_MTHS, MUKIM_CONSTRUCT.PROJ_APPROVAL_DATE \
              , MUKIM_CONSTRUCT.WORK_TYPE]

    projRef = {}
    try:
        print("Starting search cursor...")
        with arcpy.da.SearchCursor(NFDataSource.MUKIM_CONSTRUCT, fields, expression) as cursor:
            for row in cursor:
                PROJ_REF = row[12]

                if not PROJ_REF in projRef:
                    projRef[PROJ_REF] = Project()

                projRef[PROJ_REF].BUILDER_FIRM_NAME = row[0].upper()
                projRef[PROJ_REF].BUILDER_NAME = row[1].upper()
                projRef[PROJ_REF].BUILDER_TEL = row[2].upper()
                projRef[PROJ_REF].BUILDER_EMAIL = row[20].upper()
                projRef[PROJ_REF].BUILDER_ADDR = row[21].upper()

                projRef[PROJ_REF].LOT_KEY.append(row[3])

                projRef[PROJ_REF].OWNER_EMAIL = row[4]
                projRef[PROJ_REF].OWNER_FIRM_NAME = row[5].upper()
                projRef[PROJ_REF].OWNER_NAME = row[6].upper()
                projRef[PROJ_REF].OWNER_ADDR = row[22].upper()
                projRef[PROJ_REF].OWNER_TEL = row[23].upper()

                projRef[PROJ_REF].PERMIT_DATE = row[7]

                projRef[PROJ_REF].PE_NAME = row[8].upper()
                projRef[PROJ_REF].PE_EMAIL = row[24].upper()
                projRef[PROJ_REF].PE_FIRM_NAME = row[25].upper()
                projRef[PROJ_REF].PE_ADDR = row[26].upper()
                projRef[PROJ_REF].PE_TEL = row[27].upper()

                projRef[PROJ_REF].ARCHITECT_NAME = row[28].upper()
                projRef[PROJ_REF].ARCHITECT_EMAIL = row[29].upper()
                projRef[PROJ_REF].ARCHITECT_FIRM_NAME = row[30].upper()
                projRef[PROJ_REF].ARCHITECT_ADDR = row[31].upper()
                projRef[PROJ_REF].ARCHITECT_TEL = row[32].upper()


                projRef[PROJ_REF].PROJ_PARENT_CWDCATCHMENT = row[9]
                projRef[PROJ_REF].PROJ_PARENT_WRPCATCHMENT = row[10]
                projRef[PROJ_REF].PROJ_PARENT_WSNDEPOT = row[11]
                projRef[PROJ_REF].PROJ_REF_NO = row[12]
                projRef[PROJ_REF].PROJ_TITLE = row[13]
                projRef[PROJ_REF].PROJ_TOT_AREA += row[14]
                projRef[PROJ_REF].PROJ_END_DATE = row[15]
                projRef[PROJ_REF].HOUSE_BLK_NO = row[16]
                projRef[PROJ_REF].ROAD_NAME = row[17]
                projRef[PROJ_REF].BUILDING_NAME = row[18]
                projRef[PROJ_REF].POSTAL_CODE = row[19]
                projRef[PROJ_REF].PERMIT_WORK_TYPE = row[33]
                projRef[PROJ_REF].WORK_TYPE = row[37]
                projRef[PROJ_REF].PROJ_COST = row[34]
                projRef[PROJ_REF].PROJ_DURATION_MTHS = row[35]
                projRef[PROJ_REF].PROJ_APPROVAL_DATE = row[36]


        print("End search cursor")
        return projRef

    except Exception as err:
        Error_Handler("Query Projects Failed")
        print(err)


def CreateProjects(projRef):

    """
    Create project folder, generate project plans and report
    :param projRef: a list of project records
    :return:
    """

    print("Creating projects")
    # make root directory with permit date
    if (len(projRef) == 0):
        print("No project available")
        return

    dt = str(projRef.values()[0].PERMIT_DATE).split(" ")[0]
    project_root = os.path.join(os.path.dirname(os.path.abspath(__file__)), os.path.join(NFOutputPath.OUTPUT, dt))
    if not os.path.exists(project_root):
        os.makedirs(project_root)

    summary = []

    try:
        edit = arcpy.da.Editor(NFDataSource.COMMONGIS_CONN)
        edit.startEditing(False, False)
        edit.startOperation()
        editCounter = 0

        # create one folder for each project ref
        for key in projRef:
            arcpy.AddMessage(key)

            project_folder = os.path.join(project_root, key)

            # if the project folder already contains plans - skip; this project has been imported before; will not be included in new MaxDia report too
            if not os.path.exists(project_folder):
                os.makedirs(project_folder)
                file_list = []
            else:
                file_list = os.listdir(project_folder)

            pdf_exists = False

            for f in file_list:

                if ".pdf" in f:
                    arcpy.AddMessage("PDF in project folder: " + project_folder)
                    pdf_exists = True
                    break
            if pdf_exists:
                continue

            cwdPdf = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                  os.path.join(project_folder, "{0}-{1}.pdf".format(key, NFAnnex.CWD)))

            UpdateCWD(projRef[key])
            GenerateCWD(arcpy.mapping.MapDocument(NFTemplate.CWDTEMPLATE), projRef[key], cwdPdf, [], "")

            wsnPdf = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                  os.path.join(project_folder, "{0}-{1}.pdf".format(key, NFAnnex.WSN)))
            wsnasset = UpdateWSN(projRef[key])

            if (len(wsnasset) > 0):
                # GenerateCWD(arcpy.mapping.MapDocument(NFTemplate.WSNTEMPLATE), projRef[key], wsnPdf, wsnasset, "WSN")
                print "wsnasset"

            wrnPdf = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                  os.path.join(project_folder, "{0}-{1}.pdf".format(key, NFAnnex.WRN)))
            wrnasset = UpdateWRN(projRef[key])

            if (len(wrnasset) > 0):
                GenerateCWD(arcpy.mapping.MapDocument(NFTemplate.WRNTEMPLATE), projRef[key], wrnPdf, wrnasset, "WRN")
                summary.extend(GenerateReport(projRef[key], wrnasset))

            editCounter = editCounter + 1

            if (editCounter == 10):
                print ("intermediate save @ 10")
                edit.stopOperation()
                edit.stopEditing(True)

                edit.startEditing(False, False)
                edit.startOperation()
                editCounter = 0

        edit.stopOperation()
        edit.stopEditing(True)
        del edit

        max_dia_report_name = GenerateMaxDiaReport(summary, dt, projRef)
        arcpy.AddMessage("END Creating projects")
        arcpy.AddMessage(max_dia_report_name)
        print("Generated maxdia report")
        return max_dia_report_name

    except Exception as err:
        print err
        Error_Handler("Create Projects Failed")
        Error_Handler(repr(traceback.format_exc()))


def GenerateCWD(mxd, project, pdf, asset, dept):
    # prepare mxd for printing
    # set up query def on bca mukim lot

    dataFrame = arcpy.mapping.ListDataFrames(mxd)[0]
    lyr = arcpy.mapping.ListLayers(mxd, NFLayerName.MUKIM_CONSTRUCT, dataFrame)[0]
    lyr.definitionQuery = NFQuery.PROJ_REF.format(project.PROJ_REF_NO, project.PERMIT_DATE)

    if (dept == "WRN"):
        sewer = ""
        pmain = ""
        dtss = ""

        soidList = []
        poidList = []
        doidList = []

        for x in asset:
            soidList.extend(x.INTERSECTING_SEWER)
            poidList.extend(x.INTERSECTING_PMAIN)
            doidList.extend(x.INTERSECTING_DTSS)

        sllyr = arcpy.mapping.ListLayers(mxd, NFLayerName.AFFECTED_SEWER_LINE_L900, dataFrame)[0]
        smlyr = arcpy.mapping.ListLayers(mxd, NFLayerName.AFFECTED_SEWER_LINE_M900, dataFrame)[0]
        pllyr = arcpy.mapping.ListLayers(mxd, NFLayerName.AFFECTED_PUMPING_MAIN_L900, dataFrame)[0]
        pmlyr = arcpy.mapping.ListLayers(mxd, NFLayerName.AFFECTED_PUMPING_MAIN_M900, dataFrame)[0]
        dtsslyr = arcpy.mapping.ListLayers(mxd, NFLayerName.AFFECTED_DTSS, dataFrame)[0]

        if (len(soidList) > 0):
            sewer = ",".join([str(w) for w in soidList])

            sllyr.definitionQuery = NFQuery.SEWER_L900.format(sewer)
            smlyr.definitionQuery = NFQuery.SEWER_M900.format(sewer)

            sllyr.visible = True
            smlyr.visible = True
        else:
            sllyr.visible = False
            smlyr.visible = False

        if (len(poidList) > 0):
            pmain = ",".join([str(v) for v in poidList])

            pllyr.definitionQuery = NFQuery.PUMPMAIN_L900.format(pmain)
            pmlyr.definitionQuery = NFQuery.PUMPMAIN_M900.format(pmain)

            pllyr.visible = True
            pmlyr.visible = True
        else:
            pllyr.visible = False
            pmlyr.visible = False

        if (len(doidList) > 0):
            dtss = ",".join([str(y) for y in doidList])

            dtsslyr.definitionQuery = NFQuery.DTSS.format(dtss)
            dtsslyr.visible = True
        else:
            dtsslyr.visible = False

    if (dept == "WSN"):
        watermainOid = ""
        tunnelOid = ""

        # highlight all the sewers
        woidList = []
        toidList = []

        for x in asset:
            woidList.extend(x.INTERSECTING_WATERMAIN)
            toidList.extend(x.INTERSECTING_TUNNEL)

        wllyr = arcpy.mapping.ListLayers(mxd, NFLayerName.AFFECTED_WATERMAIN_LINE_L900, dataFrame)[0]
        wmlyr = arcpy.mapping.ListLayers(mxd, NFLayerName.AFFECTED_WATERMAIN_LINE_M900, dataFrame)[0]

        if (len(woidList) > 0):
            watermainOid = ",".join([str(m) for m in woidList])

            wllyr.definitionQuery = NFQuery.WATERMAIN_L900.format(watermainOid)
            wmlyr.definitionQuery = NFQuery.WATERMAIN_M900.format(watermainOid)

            wllyr.visible = True
            wmlyr.visible = True
        else:
            wllyr.visible = False
            wmlyr.visible = False

        tunnellyr = arcpy.mapping.ListLayers(mxd, NFLayerName.AFFECTED_WATERMAIN_TUNNEL, dataFrame)[0]
        if (len(toidList) > 0):
            tunnelOid = ",".join([str(n) for n in toidList])

            tunnellyr.definitionQuery = NFQuery.TUNNEL.format(tunnelOid)

            tunnellyr.visible = True
        else:
            tunnellyr.visible = False

    for element in arcpy.mapping.ListLayoutElements(mxd, "TEXT_ELEMENT"):
        if element.name == NFTemplate.PROJECT_REFERENCE:
            element.text = project.PROJ_REF_NO
            break

    dataFrame.extent = lyr.getExtent(True)
    dataFrame.scale = dataFrame.scale * 1.2

    if (dataFrame.scale < NFMapParameters.MIN_LIMIT):
        dataFrame.scale = NFMapParameters.MIN_LIMIT
        arcpy.RefreshActiveView()
        arcpy.mapping.ExportToPDF(mxd, pdf)

    elif (dataFrame.scale > NFMapParameters.MAX_LIMIT and len(project.LOT_KEY) > 1):
        # might be multiple lot
        finalPdf = arcpy.mapping.PDFDocumentCreate(pdf)
        for lot in project.LOT_KEY:
            path = os.path.dirname(pdf)
            tmpPdf = os.path.join(path, "{0}_{1}.pdf".format(project.PROJ_REF_NO, lot))
            lyr.definitionQuery = NFQuery.LOT_KEY.format(lot, project.PROJ_REF_NO, project.PERMIT_DATE)

            dataFrame.extent = lyr.getExtent(True)

            if (dataFrame.scale < NFMapParameters.MIN_LIMIT):
                dataFrame.scale = NFMapParameters.MIN_LIMIT
            else:
                dataFrame.scale = dataFrame.scale * 1.2

            arcpy.RefreshActiveView()
            arcpy.mapping.ExportToPDF(mxd, tmpPdf)
            finalPdf.appendPages(tmpPdf)
            os.remove(tmpPdf)
            del tmpPdf
    else:
        arcpy.RefreshActiveView()
        arcpy.mapping.ExportToPDF(mxd, pdf)

    del mxd


def UpdateCWD(project):
    # insert into MUKIM_CONSULT_D
    # PUBLIC_DRAIN_WITHIN_SITE if drain intersect mukim
    # drain category != Common Drain ==1
    mxd = arcpy.mapping.MapDocument(NFTemplate.CWDTEMPLATE)
    dataFrame = arcpy.mapping.ListDataFrames(mxd)[0]
    mukimlyr = arcpy.mapping.ListLayers(mxd, NFLayerName.MUKIM_CONSTRUCT, dataFrame)[0]
    drainLyr = arcpy.mapping.ListLayers(mxd, NFLayerName.DRAIN_LINE, dataFrame)[0]

    public_Drain = 0
    intersect_Mukim = []

    for lot in project.LOT_KEY:
        mukimlyr.definitionQuery = NFQuery.LOT_KEY.format(lot, project.PROJ_REF_NO, project.PERMIT_DATE)

        arcpy.SelectLayerByLocation_management(drainLyr, "INTERSECT", mukimlyr)
        count = int(arcpy.GetCount_management(drainLyr).getOutput(0))

        # there are intersecting drains
        if (count > 0):
            # loop thru the drains to check if common drain
            # if category of drain <> Common Drain
            # public drain =1 else 0
            intersect_Mukim.append(lot)

            with arcpy.da.SearchCursor(drainLyr, CWDDRAINLINE.CATEGORY) as cursor:
                for row in cursor:

                    if (public_Drain == 1): break

                    CATEGORY = row[0]
                    public_Drain = 1 if CATEGORY <> CWDDRAINLINE.COMMON_DRAIN else 0

        arcpy.SelectLayerByAttribute_management(drainLyr, "CLEAR_SELECTION")

    fields = [MUKIM_CONSULT_D.PROJ_REF_NO, MUKIM_CONSULT_D.PROJ_TOT_AREA, MUKIM_CONSULT_D.PROJ_PARENT_CWDCATCHMENT,
              MUKIM_CONSULT_D.PUBLIC_DRAIN_WITHIN_SITE, MUKIM_CONSULT_D.PERMIT_DATE \
        , MUKIM_CONSULT_D.PROJ_END_DATE, MUKIM_CONSULT_D.PROJ_LOT_KEYS, MUKIM_CONSULT_D.NF_PARAM_BUFFDIST,
              MUKIM_CONSULT_D.CONTRACTOR_OIC, MUKIM_CONSULT_D.CONTRACTOR_OIC_NO \
        , MUKIM_CONSULT_D.DEVELOPER_OIC, MUKIM_CONSULT_D.DEVELOPER_OIC_EMAIL, MUKIM_CONSULT_D.DEVELOPER_AGENCY,
              MUKIM_CONSULT_D.PROJ_CWD_CLOSED_DATE, MUKIM_CONSULT_D.PROJ_TITLE \
        , MUKIM_CONSULT_D.CONTRACTOR_AGENCY, MUKIM_CONSULT_D.PE_OIC, MUKIM_CONSULT_D.NF_EMAIL_SENT_STATUS,
              MUKIM_CONSULT_D.NF_LETTER_IGNORE_STATUS \
        , MUKIM_CONSULT_D.NF_LETTER_APPROVE_STATUS, MUKIM_CONSULT_D.NF_LETTER_VIEW_STATUS]

    with arcpy.da.InsertCursor(NFDataSource.MUKIM_CONSULT_D, fields) as cursor:
        print project.PROJ_REF_NO
        cursor.insertRow([project.PROJ_REF_NO, project.PROJ_TOT_AREA, project.PROJ_PARENT_CWDCATCHMENT, public_Drain,
                          project.PERMIT_DATE \
                             , project.PROJ_END_DATE, ','.join(str(lot) for lot in list(set(intersect_Mukim))), 0,
                          project.BUILDER_NAME, project.BUILDER_TEL \
                             , project.OWNER_NAME, project.OWNER_EMAIL, project.OWNER_FIRM_NAME, project.PROJ_END_DATE,
                          project.PROJ_TITLE \
                             , project.BUILDER_FIRM_NAME, project.PE_NAME, "NOT SENT", "NO", "NO", "NO"])


def UpdateWSN(project):
    pw_dist = pw_trans = nw_dist = nw_trans = iw_dist = iw_trans = rw_dist = rw_trans = 0
    intersect_Mukim = []
    asset = []
    watermain = 0
    tunnel = 0

    mxd = arcpy.mapping.MapDocument(NFTemplate.WSNTEMPLATE)
    dataFrame = arcpy.mapping.ListDataFrames(mxd)[0]

    mukimlyr = arcpy.mapping.ListLayers(mxd, NFLayerName.MUKIM_CONSTRUCT, dataFrame)[0]
    waterL900lyr = arcpy.mapping.ListLayers(mxd, NFLayerName.WATERMAIN_LINE_L900, dataFrame)[0]
    waterM900lyr = arcpy.mapping.ListLayers(mxd, NFLayerName.WATERMAIN_LINE_M900, dataFrame)[0]
    watertunnellyr = arcpy.mapping.ListLayers(mxd, NFLayerName.WATERMAIN_TUNNEL, dataFrame)[0]

    lyrList = [waterL900lyr, waterM900lyr, watertunnellyr]

    for lot in project.LOT_KEY:
        mukimLot = MukimLot(lot)
        mukimlyr.definitionQuery = NFQuery.LOT_KEY.format(lot, project.PROJ_REF_NO, project.PERMIT_DATE)

        arcpy.SelectLayerByLocation_management(waterL900lyr, "WITHIN_A_DISTANCE", mukimlyr, NFQuery.WSN_WITHIN_L900,
                                               "NEW_SELECTION")
        cL900 = int(arcpy.GetCount_management(waterL900lyr).getOutput(0))

        arcpy.SelectLayerByLocation_management(waterM900lyr, "WITHIN_A_DISTANCE", mukimlyr, NFQuery.WSN_WITHIN_M900,
                                               "NEW_SELECTION")
        cM900 = int(arcpy.GetCount_management(waterM900lyr).getOutput(0))

        arcpy.SelectLayerByLocation_management(watertunnellyr, "WITHIN_A_DISTANCE", mukimlyr, NFQuery.WSN_WITHIN_TUNNEL,
                                               "NEW_SELECTION")
        cT = int(arcpy.GetCount_management(watertunnellyr).getOutput(0))

        # there are intersecting sewer
        if (cL900 > 0 or cM900 > 0):
            # loop thru the sewer to get the diameter

            with arcpy.da.SearchCursor(waterL900lyr, [WSNWATERMAINLINE.OBJECTID]) as cursor:
                for row in cursor:
                    OID = row[0]
                    watermain = 1

                    mukimLot.INTERSECTING_WATERMAIN.append(OID)

            with arcpy.da.SearchCursor(waterM900lyr, [WSNWATERMAINLINE.OBJECTID]) as cursor:
                for row in cursor:
                    OID = row[0]
                    watermain = 1

                    mukimLot.INTERSECTING_WATERMAIN.append(OID)

        if (cT > 0):
            # loop thru
            with arcpy.da.SearchCursor(watertunnellyr, [WSNWATERMAINLINE.OBJECTID]) as cursor:
                for row in cursor:
                    OID = row[0]
                    mukimLot.INTERSECTING_TUNNEL.append(OID)
                    tunnel = 1

        if (watermain > 0 or tunnel > 0):
            asset.append(mukimLot)

        # there are intersecting water main
        if ((cM900 + cL900 + cT) > 0):
            intersect_Mukim.append(lot)

            # loop thru the water mains to get the type
            for layer in lyrList:
                for row in arcpy.da.SearchCursor(layer, [WSNWATERMAINLINE.CLASS, WSNWATERMAINLINE.WATER_TYPE]):
                    if (
                            pw_dist == 1 and pw_trans == 1 and nw_dist == 1 and nw_trans == 1 and iw_dist == 1 and iw_trans == 1 and rw_dist == 1 and rw_trans == 1): break

                    CLASS = row[0]
                    WATER_TYPE = row[1]

                    pw_dist = 1 if (
                                CLASS == WSNWATERMAINLINE.POTABLE_WATER and WATER_TYPE == WSNWATERMAINLINE.DISTRIBUTION) else 0
                    pw_trans = 1 if (
                                CLASS == WSNWATERMAINLINE.POTABLE_WATER and WATER_TYPE == WSNWATERMAINLINE.TRANSMISSION) else 0
                    nw_dist = 1 if (
                                CLASS == WSNWATERMAINLINE.NEW_WATER and WATER_TYPE == WSNWATERMAINLINE.DISTRIBUTION) else 0
                    nw_trans = 1 if (
                                CLASS == WSNWATERMAINLINE.NEW_WATER and WATER_TYPE == WSNWATERMAINLINE.TRANSMISSION) else 0
                    iw_dist = 1 if (
                                CLASS == WSNWATERMAINLINE.INDUSTRIAL_WATER and WATER_TYPE == WSNWATERMAINLINE.DISTRIBUTION) else 0
                    iw_trans = 1 if (
                                CLASS == WSNWATERMAINLINE.INDUSTRIAL_WATER and WATER_TYPE == WSNWATERMAINLINE.TRANSMISSION) else 0
                    rw_dist = 1 if (
                                CLASS == WSNWATERMAINLINE.RAW_WATER and WATER_TYPE == WSNWATERMAINLINE.DISTRIBUTION) else 0
                    rw_trans = 1 if (
                                CLASS == WSNWATERMAINLINE.RAW_WATER and WATER_TYPE == WSNWATERMAINLINE.TRANSMISSION) else 0

                arcpy.SelectLayerByAttribute_management(layer, "CLEAR_SELECTION")

    if (len(intersect_Mukim) > 0):
        fields = [MUKIM_CONSULT_W.PROJ_REF_NO, MUKIM_CONSULT_W.PROJ_TOT_AREA, MUKIM_CONSULT_W.PROJ_PARENT_WSNDEPOT,
                  MUKIM_CONSULT_W.PROJ_TITLE, MUKIM_CONSULT_W.PERMIT_DATE \
            , MUKIM_CONSULT_W.PROJ_END_DATE, MUKIM_CONSULT_W.PROJ_LOT_KEYS, MUKIM_CONSULT_W.NF_PARAM_BUFFDIST,
                  MUKIM_CONSULT_W.PW_DIST, MUKIM_CONSULT_W.PW_TRANS \
            , MUKIM_CONSULT_W.NW_DIST, MUKIM_CONSULT_W.NW_TRANS, MUKIM_CONSULT_W.IW_DIST, MUKIM_CONSULT_W.IW_TRANS,
                  MUKIM_CONSULT_W.RW_DIST, MUKIM_CONSULT_W.RW_TRANS \
            , MUKIM_CONSULT_W.NF_EMAIL_SENT_STATUS, MUKIM_CONSULT_W.NF_LETTER_IGNORE_STATUS,
                  MUKIM_CONSULT_W.NF_LETTER_APPROVE_STATUS, MUKIM_CONSULT_W.NF_LETTER_VIEW_STATUS]

        with arcpy.da.InsertCursor(NFDataSource.MUKIM_CONSULT_W, fields) as cursor:
            print project.PROJ_REF_NO
            cursor.insertRow(
                [project.PROJ_REF_NO, project.PROJ_TOT_AREA, project.PROJ_PARENT_WSNDEPOT, project.PROJ_TITLE,
                 project.PERMIT_DATE \
                    , project.PROJ_END_DATE, ','.join(str(lot) for lot in list(set(intersect_Mukim))), 0, pw_dist,
                 pw_trans \
                    , nw_dist, nw_trans, iw_dist, iw_trans, rw_dist, rw_trans \
                    , "NOT SENT", "NO", "NO", "NO"])

    return asset


def UpdateWRN(project):
    """
    Insert project site polygons that intersect with WRN asset into WRN table

    :param project: specific project record
    :return: list of WRN assets intersecting with the project site
    """

    intersect_Mukim = []
    asset = []
    pumpingmain = dtss = sewer = 0

    mxd = arcpy.mapping.MapDocument(NFTemplate.WRNTEMPLATE)
    dataFrame = arcpy.mapping.ListDataFrames(mxd)[0]

    mukimlyr = arcpy.mapping.ListLayers(mxd, NFLayerName.MUKIM_CONSTRUCT, dataFrame)[0]
    sewerL900lyr = arcpy.mapping.ListLayers(mxd, NFLayerName.SEWER_LINE_L900, dataFrame)[0]
    sewerM900lyr = arcpy.mapping.ListLayers(mxd, NFLayerName.SEWER_LINE_M900, dataFrame)[0]
    pumpingL900lyr = arcpy.mapping.ListLayers(mxd, NFLayerName.PUMPING_MAIN_L900, dataFrame)[0]
    pumpingM900lyr = arcpy.mapping.ListLayers(mxd, NFLayerName.PUMPING_MAIN_M900, dataFrame)[0]
    dtsslyr = arcpy.mapping.ListLayers(mxd, NFLayerName.DTSS, dataFrame)[0]

    lyrList = [sewerL900lyr, sewerM900lyr, pumpingL900lyr, pumpingM900lyr, dtsslyr]

    for lot in project.LOT_KEY:
        mukimLot = MukimLot(lot)

        mukimlyr.definitionQuery = NFQuery.LOT_KEY.format(lot, project.PROJ_REF_NO, project.PERMIT_DATE)

        arcpy.SelectLayerByLocation_management(sewerL900lyr, "WITHIN_A_DISTANCE", mukimlyr, NFQuery.WSN_WITHIN_L900,
                                               "NEW_SELECTION")
        sL900 = int(arcpy.GetCount_management(sewerL900lyr).getOutput(0))

        arcpy.SelectLayerByLocation_management(sewerM900lyr, "WITHIN_A_DISTANCE", mukimlyr, NFQuery.WSN_WITHIN_M900,
                                               "NEW_SELECTION")
        sM900 = int(arcpy.GetCount_management(sewerM900lyr).getOutput(0))

        arcpy.SelectLayerByLocation_management(pumpingL900lyr, "WITHIN_A_DISTANCE", mukimlyr, NFQuery.WSN_WITHIN_L900,
                                               "NEW_SELECTION")
        pL900 = int(arcpy.GetCount_management(pumpingL900lyr).getOutput(0))

        arcpy.SelectLayerByLocation_management(pumpingM900lyr, "WITHIN_A_DISTANCE", mukimlyr, NFQuery.WSN_WITHIN_M900,
                                               "NEW_SELECTION")
        pM900 = int(arcpy.GetCount_management(pumpingM900lyr).getOutput(0))

        arcpy.SelectLayerByLocation_management(dtsslyr, "WITHIN_A_DISTANCE", mukimlyr, NFQuery.WSN_WITHIN_TUNNEL,
                                               "NEW_SELECTION")
        sT = int(arcpy.GetCount_management(dtsslyr).getOutput(0))

        # there are intersecting sewer
        if (sL900 > 0 or sM900 > 0):
            # loop thru the sewer to get the diameter

            with arcpy.da.SearchCursor(sewerL900lyr, [WRNSEWERLINE.OBJECTID, WRNSEWERLINE.DIA]) as cursor:
                for row in cursor:
                    OID = row[0]
                    DIA = row[1]
                    sewer = 1

                    mukimLot.INTERSECTING_SEWER.append(OID)

            with arcpy.da.SearchCursor(sewerM900lyr, [WRNSEWERLINE.OBJECTID, WRNSEWERLINE.DIA]) as cursor:
                for row in cursor:
                    OID = row[0]
                    DIA = row[1]
                    sewer = 1

                    mukimLot.INTERSECTING_SEWER.append(OID)

        if (pL900 > 0 or pM900 > 0):
            # loop thru the sewer to get the diameter
            with arcpy.da.SearchCursor(pumpingL900lyr, [WRNPUMPMAIN.OBJECTID, WRNPUMPMAIN.DIA]) as cursor:
                for row in cursor:
                    OID = row[0]
                    DIA = row[1]
                    pumpingmain = 1

                    mukimLot.INTERSECTING_PMAIN.append(OID)

            with arcpy.da.SearchCursor(pumpingM900lyr, [WRNPUMPMAIN.OBJECTID, WRNPUMPMAIN.DIA]) as cursor:
                for row in cursor:
                    OID = row[0]
                    DIA = row[1]

                    pumpingmain = 1
                    mukimLot.INTERSECTING_PMAIN.append(OID)

        if (sT > 0):
            # loop thru
            with arcpy.da.SearchCursor(dtsslyr, [WRNPUMPMAIN.OBJECTID]) as cursor:
                for row in cursor:
                    OID = row[0]
                    mukimLot.INTERSECTING_DTSS.append(OID)
                    dtss = 1

        # there are intersecting sewers, dtss
        if ((sM900 + sL900 + sT + pL900 + pM900) > 0):
            intersect_Mukim.append(lot)

        if (pumpingmain > 0 or sewer > 0 or dtss > 0):
            asset.append(mukimLot)

        for layer in lyrList:
            arcpy.SelectLayerByAttribute_management(layer, "CLEAR_SELECTION")

    if (len(intersect_Mukim) > 0):
        fields = [MUKIM_CONSULT_S.PROJ_REF_NO, MUKIM_CONSULT_S.PROJ_TOT_AREA, MUKIM_CONSULT_S.PROJ_TITLE,
                  MUKIM_CONSULT_S.PERMIT_DATE \
            , MUKIM_CONSULT_S.PROJ_END_DATE, MUKIM_CONSULT_S.PROJ_LOT_KEYS, MUKIM_CONSULT_S.NF_PARAM_BUFFDIST \
            , MUKIM_CONSULT_S.SEWER_EXIST, MUKIM_CONSULT_S.PMAIN_EXIST, MUKIM_CONSULT_S.NF_EMAIL_SENT_STATUS \
            , MUKIM_CONSULT_S.NF_LETTER_IGNORE_STATUS, MUKIM_CONSULT_S.NF_LETTER_APPROVE_STATUS,
                  MUKIM_CONSULT_S.NF_LETTER_VIEW_STATUS]

        with arcpy.da.InsertCursor(NFDataSource.MUKIM_CONSULT_S, fields) as cursor:
            print project.PROJ_REF_NO
            cursor.insertRow([project.PROJ_REF_NO, project.PROJ_TOT_AREA, project.PROJ_TITLE, project.PERMIT_DATE \
                                 , project.PROJ_END_DATE, ','.join(str(lot) for lot in list(set(intersect_Mukim))), 0 \
                                 , sewer, pumpingmain, "NOT SENT", "NO", "NO", "NO"])

    return asset


def GenerateReport(project, asset):

    """
    Generate a summary of WRN assets that intersect with project
    :param project: one project record
    :param asset: list of intersecting WRN assets
    :return: a detailed summary of WRN assets that intersect with the project
    """

    try:
        wrnMxd = arcpy.mapping.MapDocument(NFTemplate.WRNTEMPLATE)
        report_root = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                   os.path.join(NFOutputPath.REPORT_OUTPUT, str(project.PERMIT_DATE).split(" ")[0]))

        if not os.path.exists(report_root):
            os.makedirs(report_root)

        mukimList = []

        for mukim in asset:
            # query the sewer line or pumping main
            # query the rehpipe for reh_date
            soidList = ','.join(str(x) for x in mukim.INTERSECTING_SEWER)
            poidList = ','.join(str(x) for x in mukim.INTERSECTING_PMAIN)

            if (soidList):
                fields = [WRNSEWERLINE.G3E_FID, WRNSEWERLINE.DIA, WRNSEWERLINE.MAT, WRNSEWERLINE.MLEN,
                          WRNSEWERLINE.REH_STA, WRNSEWERLINE.REMARKS, WRNSEWERLINE.STATE, WRNSEWERLINE.STYPE,
                          WRNSEWERLINE.YR_COMM]
                with arcpy.da.SearchCursor(NFDataSource.WRNSEWERLINE, fields,
                                           NFReportQuery.SEWER.format(soidList)) as cursor:
                    for row in cursor:
                        summary = WRNSummary()
                        summary.PROJ_REF_NO = project.PROJ_REF_NO
                        summary.LOT_KEY = mukim.LOT_KEY

                        summary.G3E_FID = row[0]
                        summary.DIA = row[1]
                        summary.MAT = row[2]
                        summary.LEN = row[3]
                        summary.REH_STA = row[4]
                        summary.REMARKS = row[5]
                        summary.STATE = row[6]
                        summary.STYPE = row[7]
                        summary.YR_COMM = row[8].strftime("%Y") if row[8] <> None else None
                        summary.FEATURE_TY = WRNCMNREHPIPE.S_SEW
                        summary.REH_YEAR = ""

                        with arcpy.da.SearchCursor(NFDataSource.WRNCMNREHPIPE, WRNCMNREHPIPE.REH_DATE,
                                                   NFReportQuery.REHPIPE.format(summary.G3E_FID,
                                                                                WRNCMNREHPIPE.SEWER_TYPE)) as cur:
                            for row in cur:
                                summary.REH_YEAR = row[0].strftime("%Y") if row[0] <> None else None

                                break

                        mukimList.append(summary)

            if (poidList):
                fields = [WRNPUMPMAIN.G3E_FID, WRNPUMPMAIN.DIA, WRNPUMPMAIN.MAT, WRNPUMPMAIN.MLEN, WRNPUMPMAIN.REH_STA,
                          WRNPUMPMAIN.REMARKS, WRNPUMPMAIN.STATE, WRNPUMPMAIN.STYPE, WRNPUMPMAIN.YR_COMM]
                with arcpy.da.SearchCursor(NFDataSource.WRNPUMPMAIN, fields,
                                           NFReportQuery.PUMPMAIN.format(poidList)) as cursor:
                    for row in cursor:
                        summary = WRNSummary()
                        summary.PROJ_REF_NO = project.PROJ_REF_NO
                        summary.LOT_KEY = mukim.LOT_KEY

                        summary.G3E_FID = row[0]
                        summary.DIA = row[1]
                        summary.MAT = row[2]
                        summary.LEN = row[3]
                        summary.REH_STA = row[4]
                        summary.REMARKS = row[5]
                        summary.STATE = row[6]
                        summary.STYPE = row[7]
                        summary.YR_COMM = row[8].strftime("%Y") if row[8] <> None else None
                        summary.FEATURE_TY = WRNCMNREHPIPE.S_PMLN
                        summary.REH_YEAR = ""

                        with arcpy.da.SearchCursor(NFDataSource.WRNCMNREHPIPE, WRNCMNREHPIPE.REH_DATE,
                                                   NFReportQuery.REHPIPE.format(summary.G3E_FID,
                                                                                WRNCMNREHPIPE.PMAIN_TYPE)) as cur:
                            for row in cur:
                                summary.REH_YEAR = row[0].strftime("%Y") if row[0] <> None else None

                                break

                        mukimList.append(summary)

        return mukimList

    except Exception as err:
        Error_Handler("Generate WRN Report Error")


def GenerateMaxDiaReport(summary, dt, projects):

    """
    Generate maxdia report
    :param summary: Summary of WRN infra
    :param dt: project permit date
    :param projects: a list of project records
    :return: NIL
    """

    arcpy.AddMessage("Start generating MaxDia report")

    try:
        # can get the max diameter in the mukim
        # how to get the proj title, project etc.
        # [[Project][Project][Project]]
        # regroup using project and mukim -> get max dia

        report_root = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                   os.path.join(NFOutputPath.REPORT_OUTPUT, dt))

        if not os.path.exists(report_root):
            os.makedirs(report_root)



        today_str = datetime.date.today().strftime("%Y%m%d")
        max_dia_report_name = NFReport_WRN.MAX_DIA_REPORT.format(dt, today_str)
        csv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                os.path.join(report_root, max_dia_report_name))
        if os.path.exists(csv_path):
            report_count = len(os.listdir(report_root))
            max_dia_report_name = max_dia_report_name.split(".")[0] + "_" + str(report_count+1) + ".csv"
            csv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                os.path.join(report_root, max_dia_report_name))

        csvWriter = csv.writer(open(csv_path, "wb"))
        csvWriter.writerow(NFReport_WRN.MAX_DIA_CSV_FIELDS)
        # ["Max Diameter", "Ref Mukim No", "Project Ref No", "Project Title", "House Blk No", "Road Name", "Building Name", "Postal Code"]

        # reorganise summary
        groups = defaultdict(list)
        for item in summary:
            REF_MUKIM = "{0}_{1}".format(item.PROJ_REF_NO, item.LOT_KEY)
            groups[REF_MUKIM].append(item.DIA)

        for group in groups:

            MAX_DIA = max(item for item in groups[group])
            PROJ_REF_NO = group.split("_")[0]
            LOT_KEY = group.split("_")[1]

            PERMIT_DATE = projects[PROJ_REF_NO].PERMIT_DATE.strftime("%Y-%m-%d")

            PROJ_TITLE = projects[PROJ_REF_NO].PROJ_TITLE
            HOUSE_BLK_NO = projects[PROJ_REF_NO].HOUSE_BLK_NO
            ROAD_NAME = projects[PROJ_REF_NO].ROAD_NAME
            BUILDING_NAME = projects[PROJ_REF_NO].BUILDING_NAME
            POSTAL_CODE = projects[PROJ_REF_NO].POSTAL_CODE

            ## ADDED BELOW FOR TSS5 CR ##
            PERMIT_WORK_TYPE = projects[PROJ_REF_NO].PERMIT_WORK_TYPE
            WORK_TYPE = projects[PROJ_REF_NO].WORK_TYPE

            OWNER_NAME = projects[PROJ_REF_NO].OWNER_NAME
            OWNER_FIRM_NAME = projects[PROJ_REF_NO].OWNER_FIRM_NAME
            OWNER_ADDRESS = projects[PROJ_REF_NO].OWNER_ADDR
            OWNER_TEL = projects[PROJ_REF_NO].OWNER_TEL
            OWNER_EMAIL = projects[PROJ_REF_NO].OWNER_EMAIL

            BUILDER_NAME = projects[PROJ_REF_NO].BUILDER_NAME
            BUILDER_FIRM_NAME = projects[PROJ_REF_NO].BUILDER_FIRM_NAME
            BUILDER_ADDRESS = projects[PROJ_REF_NO].BUILDER_ADDR
            BUILDER_TEL = projects[PROJ_REF_NO].BUILDER_TEL
            BUILDER_EMAIL = projects[PROJ_REF_NO].BUILDER_EMAIL

            PE_NAME = projects[PROJ_REF_NO].PE_NAME
            PE_FIRM_NAME = projects[PROJ_REF_NO].PE_FIRM_NAME
            PE_ADDRESS = projects[PROJ_REF_NO].PE_ADDR
            PE_TEL = projects[PROJ_REF_NO].PE_TEL
            PE_EMAIL = projects[PROJ_REF_NO].PE_EMAIL

            ARCHITECT_NAME = projects[PROJ_REF_NO].ARCHITECT_NAME
            ARCHITECT_FIRM_NAME = projects[PROJ_REF_NO].ARCHITECT_FIRM_NAME
            ARCHITECT_ADDRESS = projects[PROJ_REF_NO].ARCHITECT_ADDR
            ARCHITECT_TEL = projects[PROJ_REF_NO].ARCHITECT_TEL
            ARCHITECT_EMAIL = projects[PROJ_REF_NO].ARCHITECT_EMAIL

            PROJ_COST = projects[PROJ_REF_NO].PROJ_COST
            PROJ_DURATION = projects[PROJ_REF_NO].PROJ_DURATION_MTHS
            PROJ_APPROVALDATE = projects[PROJ_REF_NO].PROJ_APPROVAL_DATE

            # write to csv
            csvWriter.writerow(
                [PERMIT_DATE, MAX_DIA, LOT_KEY, PROJ_REF_NO, PROJ_TITLE, HOUSE_BLK_NO, ROAD_NAME, BUILDING_NAME, POSTAL_CODE,
                 PERMIT_WORK_TYPE, WORK_TYPE, OWNER_NAME, OWNER_FIRM_NAME, OWNER_ADDRESS, OWNER_TEL, OWNER_EMAIL,
                 BUILDER_NAME, BUILDER_FIRM_NAME, BUILDER_ADDRESS, BUILDER_TEL, BUILDER_EMAIL,
                 PE_NAME, PE_FIRM_NAME, PE_ADDRESS, PE_TEL, PE_EMAIL,
                 ARCHITECT_NAME, ARCHITECT_FIRM_NAME, ARCHITECT_ADDRESS, ARCHITECT_TEL, ARCHITECT_EMAIL,
                 PROJ_COST, PROJ_DURATION, PROJ_APPROVALDATE])

    except Exception as err:
        Error_Handler("Generate WRN MaxDia Report Error")

    arcpy.AddMessage("End generating MaxDia report")
    return max_dia_report_name


def ZipReport(report_name, dt):
    arcpy.AddMessage("Zipping report")
    """
    Zip and copy the max dia report to shared folder for download
    :param dt: permit report date time
    :return: NIL
    """
    import shutil
    from shutil import copyfile
    dt_time = dt.strftime("%Y-%m-%d")

    try:

        # output_filename = NFReport_WRN.MAX_DIA_REPORT_ZIP.format(dt_time, today_str)
        final_output = os.path.join(NFOutputPath.SHARED_OUTPUT, report_name + ".zip")
        report_root = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                   os.path.join(NFOutputPath.REPORT_OUTPUT, dt_time))
        csv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                os.path.join(report_root, report_name))

        shutil.make_archive(final_output[:-4], "zip", report_root)


    except Exception as err:
        Error_Handler("Zip Report Error")
        arcpy.AddMessage(err)
        arcpy.AddMessage(traceback.format_exc())


def run(dt):
    """
    function to generate project site plans and MaxDia report given the permit date
    Param dt: permit report date in datetime
    return: None
    """

    print("Generating reports for date: " + repr(dt))
    arcpy.AddMessage("Generating reports for date: " + repr(dt))
    try:

        # get list of projects
        arcpy.AddMessage("Querying mukim lot")
        projRef = QueryMukimLot(dt)
        arcpy.AddMessage(len(projRef))

        # print the template
        arcpy.AddMessage("Creating projects")
        max_dia_report_name = CreateProjects(projRef)
        arcpy.AddMessage(max_dia_report_name)
        # sys.exit()

        if (len(projRef) == 0):
            return

        dt_str = (str(projRef.values()[0].PERMIT_DATE)).split(" ")[0]
        project_root = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                    os.path.join(NFOutputPath.OUTPUT, dt_str))

        # copy to gis share
        output_root = os.path.join(NFOutputPath.SCRIPT_PATH, os.path.join(NFOutputPath.OUTPUT))
        output_folder = os.path.join(output_root, dt_str)
        if os.path.exists(output_folder):
            shutil.rmtree(output_folder)

        shutil.copytree(project_root, output_folder)
        print project_root
        print output_folder

        # check if MaxDia report exists
        # if not, trigger the script again to generate report
        arcpy.AddMessage("Check report exists")

        report_exists = check_report_exists(max_dia_report_name, dt)
        arcpy.AddMessage("Checking maxdia report")
        arcpy.AddMessage(report_exists)
        count_try = 0

        while (not report_exists) and count_try <= 3:
            arcpy.AddMessage("Re-generating report")
            import ReportGeneration_Adhoc_WithoutProjects as rerun
            rerun.run(dt)
            report_exists = check_report_exists(max_dia_report_name, dt)
            count_try += 1

        # if report is generated, zip and send completion email
        if report_exists:
            ZipReport(max_dia_report_name, dt)

        else:

            pass

            # inform support
            # send_email.sendEmail(NFEmail.EMAIL_SENDER, NFErrorReport.EMAIL_SUPPORT, "Error generating MaxDia Report - Permit Date "+dt.strftime("%Y%m%d"), "")


    except Exception as err:
        Error_Handler("Report Generation Error")
        Error_Handler(traceback.format_exc())
        arcpy.AddMessage(err)
        arcpy.AddMessage(traceback.format_exc())

from base.jobs import SQLJob
from pygrametl.tables import Dimension, TypeOneSlowlyChangingDimension

class FactClaim(SQLJob):
    def configure(self):
        self.source_database = 'STG'
        self.source_table = 'v8ClaimExtract'
        self.target_database = 'DW'
        self.target_table = 'FactClaim'

    def getSqlQuery(self):
        return """
             SELECT
                DCD.DimClaimDetailPK AS 'DimClaimDetailKey',
                DD.DateKey AS 'DimClaimSubmittedDateKey',
                DPH.DimPharmacyPK AS 'DimPharmacyKey',
                DC.DimCAGPK AS 'DimCAGKey',
                v8.METRIQTY,
                v8.DECIMALQTY,
                v8.DAYSSUPPLY,
                v8.WACUNITCST,
                v8.CTYPEUCOST,
                v8.PRICEQTY,
                v8.USUALNCUST,
                v8.PHRINGRCST,
                v8.PHRDISPFEE,
                v8.PHRPPSTAX,
                v8.PHRFSTAX,
                v8.PHRSLSTAX,
                v8.PHRPATPAY,
                v8.PHRDUEAMT,
                v8.CLTINGRCST,
                v8.CLTDISPFEE,
                v8.CLTSLSTAX,
                v8.CLTPATPAY,
                v8.CLTDUEAMT,
                v8.CLTWITHHLD,
                v8.CLTFCOPAY,
                v8.CLTCOPAY,
                v8.CLTFSTAX,
                v8.CLTPATSTAX,
                v8.CLTPLNSTAX,
                CASE WHEN v8.CLAIMSTS = 'P' THEN ABS(v8.HLTHPLNAMT)
                WHEN v8.CLAIMSTS = 'X' then -1 * ABS(v8.HLTHPLNAMT)
                ELSE HLTHPLNAMT END AS `HLTHPLNAMT`,
                v8.CLMCOUNTER,
                RCRP.PassthroughRate AS 'PassthroughRate',
                CASE WHEN DCD.ClientRebateChannel = 'Mail 30' THEN RCRP.Mail30
                WHEN DCD.ClientRebateChannel = 'Mail 90' THEN RCRP.Mail90
                WHEN DCD.ClientRebateChannel = 'Retail 30' THEN RCRP.Retail30
                WHEN DCD.ClientRebateChannel = 'Retail 90' THEN RCRP.Retail90
                WHEN DCD.ClientRebateChannel = 'Specialty' THEN RCRP.Specialty END AS `ClientRebateMin`,
                (CASE WHEN DCD.ClientRebateChannel IN ('Mail 30', 'Specialty', 'Retail 30') THEN v8.DAYSSUPPLY * v8.CLMCOUNTER/30
                WHEN DCD.ClientRebateChannel IN ( 'Mail 90', 'Retail 90') THEN v8.DAYSSUPPLY * v8.CLMCOUNTER/90 END) AS 'ProportionalRX',

                ('PassthroughRate' * 'ProportionalRX') AS `MinClientDue`,

                CASE WHEN DCD.OptumRebateChannel = 'Mail 30' THEN  RCRP.Mail30
                WHEN DCD.OptumRebateChannel = 'Mail 90' THEN  RCRP.Mail90
                WHEN DCD.OptumRebateChannel = 'Retail 30' THEN  RCRP.Retail30
                WHEN DCD.OptumRebateChannel = 'Retail 90' THEN  RCRP.Retail90
                WHEN DCD.OptumRebateChannel = 'Specialty' THEN  RCRP.Specialty END AS 'OptumRebateMin',
                ('PassthroughRate' * 'OptumRebateMin') AS `PassthroughAmountDue`,
                CASE WHEN RCRP.CarrierID = 'LDI02' then '0.29' else 0 END AS  `RebateAdditionalFee`,
                1 AS 'RowIsCurrent',
                CURDATE() AS 'RowStartDate',
                '2100-12-31' AS 'RowEndDate',
                'Not Applicable' AS 'RowChangeReason',
                0 AS 'RowIsInferred'

                FROM (Select CLM.*, BEN.FormularyFollowed from HISTORICAL.v8ClaimExtract_HISTORICAL CLM
                Left outer join DW.REFBenefit BEN
                on CLM.carrierid = BEN.CarrierID
                and CLM.datesbm between BEN.CarrierBeginDate and BEN.CarrierEndDate
                Where CLM.datesbm between '2017-07-01' and '2017-07-31') v8

                LEFT OUTER JOIN DW.DimClaimDetail AS DCD
                ON DCD.ClaimNumber = v8.RXCLMNBR
                AND DCD.ClaimSequenceNumber = v8.CLMSEQNBR
                AND DCD.ClaimStatus = v8.CLAIMSTS

                LEFT OUTER JOIN DW.DimDate as DD
                ON DD.FullDate = v8.DATESBM

                LEFT OUTER JOIN DW.DimPharmacy AS DPH
                ON DPH.PharmacyID = v8.SRVPROVID

                LEFT OUTER JOIN DW.DimCAG as DC
                ON DC.CARRIERID = v8.CARRIERID
                AND DC.ACCOUNTID = v8.ACCOUNTID
                AND DC.GROUPID = v8.GROUPID

                LEFT OUTER JOIN DW.REFClientRebatePricing RCRP
                ON   v8.FormularyFollowed = RCRP.CarrierID
                AND v8.DATESBM between RCRP.EffDate  AND  RCRP.EndDate

                Left Outer Join DW.REFOptumRebatePricing ORP
                ON v8.Formularyfollowed = ORP.FormID
                and v8.datesbm between ORP.EffDate and ORP.EndDate LIMIT 600
        """

    def getColumnMapping(self):
        return ('DimClaimDetailKey', 'DimClaimSubmittedDateKey', 'DimPharmacyKey', 'DimCAGKey', 'METRIQTY', 'DECIMALQTY', 'DAYSSUPPLY', 'WACUNITCST', 'CTYPEUCOST', 'PRICEQTY', 'USUALNCUST', 'PHRINGRCST', 'PHRDISPFEE', 'PHRPPSTAX', 'PHRFSTAX', 'PHRSLSTAX', 'PHRPATPAY', 'PHRDUEAMT', 'CLTINGRCST', 'CLTDISPFEE', 'CLTSLSTAX', 'CLTPATPAY', 'CLTDUEAMT',\
     'CLTWITHHLD', 'CLTFCOPAY', 'CLTCOPAY', 'CLTFSTAX', 'CLTPATSTAX', 'CLTPLNSTAX', 'HLTHPLNAMT', 'CLMCOUNTER', 'PassthroughRate', 'ClientRebateMin', 'ProportionalRX', 'MinClientDue', 'OptumRebateMin', 'PassthroughAmountDue', 'RebateAdditionalFee', 'RowIsCurrent', 'RowStartDate', 'RowEndDate', 'RowChangeReason', 'RowIsInferred')


    def getTarget(self):
        FactClaim = TypeOneSlowlyChangingDimension(
            name='FactClaim',
            key='DimClaimDetailKey',
            attributes=['DimClaimSubmittedDateKey', 'DimPharmacyKey', 'DimCAGKey', 'METRIQTY', 'DECIMALQTY', 'DAYSSUPPLY', 'WACUNITCST', 'CTYPEUCOST', 'PRICEQTY', 'USUALNCUST', 'PHRINGRCST', 'PHRDISPFEE', 'PHRPPSTAX', 'PHRFSTAX', 'PHRSLSTAX', 'PHRPATPAY', 'PHRDUEAMT', \
            'CLTINGRCST', 'CLTDISPFEE', 'CLTSLSTAX', 'CLTPATPAY', 'CLTDUEAMT', 'CLTWITHHLD', 'CLTFCOPAY', 'CLTCOPAY', 'CLTFSTAX', 'CLTPATSTAX', 'CLTPLNSTAX', 'HLTHPLNAMT', 'CLMCOUNTER', 'PassthroughRate', 'ClientRebateMin', 'ProportionalRX', 'MinClientDue', 'OptumRebateMin', 'PassthroughAmountDue',\
            'RebateAdditionalFee', 'RowIsCurrent', 'RowStartDate', 'RowEndDate', 'RowChangeReason', 'RowIsInferred'],
            lookupatts=['DimPharmacyKey','DimClaimSubmittedDateKey']
            #ALL columns except the Natural Keys (lookupatts) are Type1 so no need to list them
            #PygramETL will include ALL of the non-lookupatts as type1atts
            #type1atts=[]
        )
        return FactClaim

    # Override the following method if the data needs to be transformed before insertion
    #def prepareRow(self, row):
        #return row
        #print(row)
    # Override the following method if the data needs to be transformed before insertion
    def insertRow(self, target, row):
        target.scdensure(row)
        #print(row)

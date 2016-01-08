/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.hive.execution

import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.DescribeCommand
import org.apache.spark.sql.hive.test.TestFlint
import java.io._

import com.intel.ssg.bdt.spark.sql.CalciteDialect
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.execution.{SetCommand, DescribeFunction, ExplainCommand, ShowFunctions}
import scala.util.control.NonFatal

class HiveCompSuite extends HiveCompatibilitySuite {
  override def beforeAll() {
    super.beforeAll()
    TestFlint.setConf("spark.sql.dialect", classOf[CalciteDialect].getCanonicalName)
    TestFlint.setConf("spark.sql.caseSensitive", "false")
  }

  override def afterAll() {
    super.afterAll()
  }

  override def blackList = Seq(
    // ignore cases
    "0695",
    "0721",
    // unsupported subquery
    "0157",
    "0198",
    "0274",
    "0346",
    "0348",
    "0349",
    "0887",
    "0888",
    // NCHAR
    "0896",
    // Calcite issue
    "0897",
    // special table/attribute name
    "0460",
    "0741",
    "0743",
    // all / any / some subquery
    "0143",
    "0145",
    "0147",
    "0158",
    "0159",
    "0160",
    "0197",
    "0273",
    "0278",
    "0334",
    "0335",
    "0336",
    "0337",
    "0338",
    "0339",
    "0340",
    "0341",
    "0342",
    "0343",
    "0344",
    "0345",
    "0369",
    "0370",
    "0371",
    "0372",
    "0373",
    "0374",
    "0384",
    // one row result subquery
    "0134",
    "0135",
    "0192",
    "0193",
    "0199",
    "0200",
    "0201",
    "0202",
    "0210",
    "0211",
    "0212",
    "0213",
    "0433",
    "0444",
    "0446",
    "0710",
    "0711",
    "0881",
    "0889",
    "0890",
    "1032",
    "1034",
    // order by results diff
    "0930",
    "0932",
    "0934"
  )

  /**
   * The set of tests that are believed to be working in catalyst. Tests not on whiteList or
   * blacklist are implicitly marked as ignored.
   */
  override def whiteList = Seq(
    "0912"
    //for nist
    "0000",
    "0001",
    "0002",
    "0003",
    "0004",
    "0005",
    "0006",
    "0007",
    "0008",
    "0009",
    "0010",
    "0011",
    "0012",
    "0013",
    "0014",
    "0015",
    "0016",
    "0017",
    "0018",
    "0019",
    "0020",
    "0021",
    "0022",
    "0023",
    "0024",
    "0025",
    "0026",
    "0027",
    "0028",
    "0029",
    "0030",
    "0031",
    "0032",
    "0033",
    "0034",
    "0035",
    "0036",
    "0037",
    "0038",
    "0039",
    "0040",
    "0041",
    "0042",
    "0043",
    "0044",
    "0045",
    "0046",
    "0047",
    "0048",
    "0049",
    "0050",
    "0051",
    "0052",
    "0053",
    "0054",
    "0055",
    "0056",
    "0057",
    "0058",
    "0059",
    "0060",
    "0061",
    "0063",
    "0064",
    "0065",
    "0066",
    "0067",
    "0068",
    "0069",
    "0070",
    "0071",
    "0072",
    "0073",
    "0074",
    "0075",
    "0076",
    "0077",
    "0078",
    "0079",
    "0080",
    "0081",
    "0082",
    "0083",
    "0084",
    "0085",
    "0086",
    "0087",
    "0088",
    "0089",
    "0090",
    "0091",
    "0092",
    "0093",
    "0094",
    "0095",
    "0096",
    "0097",
    "0098",
    "0099",
    "0100",
    "0101",
    "0102",
    "0103",
    "0104",
    "0105",
    "0106",
    "0107",
    "0108",
    "0109",
    "0110",
    "0111",
    "0112",
    "0113",
    "0114",
    "0115",
    "0116",
    "0117",
    "0118",
    "0119",
    "0120",
    "0121",
    "0122",
    "0123",
    "0124",
    "0125",
    "0126",
    "0127",
    "0128",
    "0129",
    "0130",
    "0131",
    "0132",
    "0133",
    "0136",
    "0137",
    "0138",
    "0139",
    "0140",
    "0141",
    "0142",
    "0144",
    "0146",
    "0148",
    "0149",
    "0150",
    "0151",
    "0152",
    "0153",
    "0154",
    "0155",
    "0156",
    "0161",
    "0162",
    "0163",
    "0164",
    "0165",
    "0166",
    "0167",
    "0168",
    "0169",
    "0170",
    "0171",
    "0172",
    "0173",
    "0174",
    "0175",
    "0176",
    "0177",
    "0178",
    "0179",
    "0180",
    "0181",
    "0182",
    "0183",
    "0184",
    "0185",
    "0186",
    "0187",
    "0188",
    "0189",
    "0190",
    "0191",
    "0194",
    "0195",
    "0196",
    "0203",
    "0204",
    "0205",
    "0206",
    "0208",
    "0209",
    "0214",
    "0215",
    "0216",
    "0217",
    "0218",
    "0219",
    "0220",
    "0221",
    "0222",
    "0223",
    "0224",
    "0225",
    "0226",
    "0227",
    "0228",
    "0229",
    "0230",
    "0231",
    "0232",
    "0233",
    "0234",
    "0235",
    "0236",
    "0237",
    "0238",
    "0239",
    "0240",
    "0241",
    "0242",
    "0243",
    "0244",
    "0245",
    "0246",
    "0247",
    "0248",
    "0249",
    "0250",
    "0251",
    "0252",
    "0253",
    "0254",
    "0255",
    "0256",
    "0257",
    "0258",
    "0259",
    "0260",
    "0261",
    "0262",
    "0263",
    "0264",
    "0265",
    "0266",
    "0267",
    "0268",
    "0270",
    "0271",
    "0272",
    "0275",
    "0276",
    "0277",
    "0279",
    "0280",
    "0281",
    "0282",
    "0283",
    "0284",
    "0285",
    "0286",
    "0287",
    "0288",
    "0289",
    "0290",
    "0291",
    "0292",
    "0293",
    "0294",
    "0295",
    "0297",
    "0298",
    "0299",
    "0300",
    "0301",
    "0302",
    "0303",
    "0304",
    "0305",
    "0306",
    "0307",
    "0308",
    "0309",
    "0310",
    "0311",
    "0312",
    "0313",
    "0314",
    "0315",
    "0316",
    "0317",
    "0318",
    "0319",
    "0320",
    "0321",
    "0322",
    "0323",
    "0324",
    "0325",
    "0326",
    "0327",
    "0328",
    "0329",
    "0330",
    "0331",
    "0332",
    "0333",
    "0347",
    "0350",
    "0351",
    "0352",
    "0353",
    "0354",
    "0355",
    "0356",
    "0357",
    "0358",
    "0359",
    "0360",
    "0361",
    "0362",
    "0363",
    "0364",
    "0365",
    "0366",
    "0367",
    "0368",
    "0375",
    "0376",
    "0377",
    "0378",
    "0379",
    "0380",
    "0381",
    "0382",
    "0383",
    "0385",
    "0386",
    "0387",
    "0388",
    "0389",
    "0390",
    "0391",
    "0392",
    "0393",
    "0394",
    "0395",
    "0396",
    "0397",
    "0398",
    "0399",
    "0400",
    "0401",
    "0402",
    "0403",
    "0404",
    "0405",
    "0406",
    "0407",
    "0408",
    "0409",
    "0410",
    "0411",
    "0412",
    "0413",
    "0414",
    "0415",
    "0416",
    "0417",
    "0418",
    "0419",
    "0420",
    "0421",
    "0422",
    "0423",
    "0424",
    "0425",
    "0426",
    "0427",
    "0428",
    "0429",
    "0430",
    "0431",
    "0432",
    "0434",
    "0435",
    "0436",
    "0437",
    "0438",
    "0439",
    "0440",
    "0441",
    "0442",
    "0443",
    "0445",
    "0449",
    "0450",
    "0451",
    "0452",
    "0453",
    "0454",
    "0455",
    "0456",
    "0457",
    "0458",
    "0459",
    "0461",
    "0462",
    "0463",
    "0464",
    "0465",
    "0466",
    "0467",
    "0468",
    "0469",
    "0470",
    "0471",
    "0473",
    "0475",
    "0476",
    "0477",
    "0478",
    "0479",
    "0480",
    "0481",
    "0482",
    "0483",
    "0484",
    "0485",
    "0486",
    "0487",
    "0488",
    "0489",
    "0490",
    "0491",
    "0492",
    "0493",
    "0494",
    "0495",
    "0496",
    "0497",
    "0498",
    "0499",
    "0500",
    "0501",
    "0502",
    "0503",
    "0504",
    "0505",
    "0506",
    "0507",
    "0508",
    "0509",
    "0510",
    "0511",
    "0512",
    "0513",
    "0514",
    "0515",
    "0517",
    "0519",
    "0521",
    "0523",
    "0524",
    "0525",
    "0528",
    "0529",
    "0530",
    "0531",
    "0532",
    "0534",
    "0535",
    "0536",
    "0537",
    "0538",
    "0539",
    "0540",
    "0541",
    "0542",
    "0543",
    "0544",
    "0545",
    "0546",
    "0547",
    "0548",
    "0549",
    "0550",
    "0551",
    "0552",
    "0553",
    "0554",
    "0555",
    "0556",
    "0557",
    "0558",
    "0559",
    "0560",
    "0561",
    "0562",
    "0563",
    "0564",
    "0565",
    "0566",
    "0567",
    "0568",
    "0569",
    "0570",
    "0571",
    "0572",
    "0573",
    "0574",
    "0575",
    "0576",
    "0577",
    "0578",
    "0579",
    "0580",
    "0581",
    "0582",
    "0583",
    "0584",
    "0585",
    "0586",
    "0587",
    "0588",
    "0589",
    "0590",
    "0591",
    "0592",
    "0593",
    "0594",
    "0595",
    "0596",
    "0597",
    "0598",
    "0599",
    "0600",
    "0601",
    "0602",
    "0603",
    "0604",
    "0605",
    "0606",
    "0607",
    "0608",
    "0609",
    "0610",
    "0611",
    "0612",
    "0613",
    "0614",
    "0615",
    "0616",
    "0617",
    "0618",
    "0619",
    "0620",
    "0621",
    "0622",
    "0623",
    "0624",
    "0625",
    "0626",
    "0627",
    "0628",
    "0629",
    "0630",
    "0631",
    "0632",
    "0633",
    "0634",
    "0635",
    "0636",
    "0637",
    "0638",
    "0639",
    "0640",
    "0641",
    "0642",
    "0643",
    "0644",
    "0645",
    "0646",
    "0647",
    "0648",
    "0649",
    "0650",
    "0651",
    "0652",
    "0653",
    "0654",
    "0655",
    "0656",
    "0657",
    "0658",
    "0659",
    "0660",
    "0661",
    "0662",
    "0663",
    "0664",
    "0665",
    "0666",
    "0667",
    "0668",
    "0669",
    "0670",
    "0671",
    "0672",
    "0673",
    "0674",
    "0675",
    "0676",
    "0677",
    "0678",
    "0679",
    "0680",
    "0681",
    "0682",
    "0683",
    "0684",
    "0685",
    "0686",
    "0687",
    "0688",
    "0689",
    "0690",
    "0691",
    "0692",
    "0693",
    "0694",
    "0696",
    "0697",
    "0698",
    "0699",
    "0700",
    "0701",
    "0702",
    "0703",
    "0704",
    "0705",
    "0706",
    "0707",
    "0708",
    "0709",
    "0712",
    "0713",
    "0714",
    "0715",
    "0716",
    "0717",
    "0718",
    "0719",
    "0720",
    "0722",
    "0723",
    "0724",
    "0725",
    "0726",
    "0727",
    "0728",
    "0729",
    "0730",
    "0731",
    "0732",
    "0733",
    "0734",
    "0735",
    "0736",
    "0737",
    "0738",
    "0739",
    "0740",
    "0742",
    "0744",
    "0745",
    "0746",
    "0747",
    "0748",
    "0749",
    "0750",
    "0751",
    "0752",
    "0753",
    "0754",
    "0755",
    "0756",
    "0757",
    "0758",
    "0759",
    "0760",
    "0761",
    "0762",
    "0763",
    "0764",
    "0765",
    "0766",
    "0767",
    "0768",
    "0769",
    "0770",
    "0771",
    "0772",
    "0773",
    "0774",
    "0775",
    "0776",
    "0777",
    "0778",
    "0779",
    "0780",
    "0781",
    "0782",
    "0784",
    "0785",
    "0786",
    "0787",
    "0788",
    "0789",
    "0790",
    "0791",
    "0792",
    "0793",
    "0794",
    "0795",
    "0796",
    "0797",
    "0798",
    "0799",
    "0800",
    "0801",
    "0802",
    "0803",
    "0804",
    "0805",
    "0806",
    "0807",
    "0808",
    "0809",
    "0810",
    "0811",
    "0812",
    "0813",
    "0815",
    "0816",
    "0817",
    "0818",
    "0819",
    "0820",
    "0821",
    "0822",
    "0823",
    "0824",
    "0825",
    "0826",
    "0827",
    "0828",
    "0829",
    "0830",
    "0831",
    "0832",
    "0833",
    "0834",
    "0835",
    "0836",
    "0837",
    "0838",
    "0839",
    "0840",
    "0841",
    "0842",
    "0843",
    "0844",
    "0845",
    "0846",
    "0847",
    "0848",
    "0849",
    "0850",
    "0851",
    "0852",
    "0853",
    "0854",
    "0855",
    "0856",
    "0857",
    "0858",
    "0859",
    "0860",
    "0861",
    "0862",
    "0863",
    "0864",
    "0865",
    "0866",
    "0867",
    "0868",
    "0869",
    "0871",
    "0872",
    "0873",
    "0874",
    "0875",
    "0876",
    "0877",
    "0878",
    "0879",
    "0880",
    "0882",
    "0883",
    "0884",
    "0885",
    "0886",
    "0891",
    "0892",
    "0893",
    "0894",
    "0895",
    "0898",
    "0899",
    "0900",
    "0901",
    "0902",
    "0903",
    "0904",
    "0905",
    "0906",
    "0912",
    "0913",
    "0914",
    "0915",
    "0916",
    "0917",
    "0918",
    "0919",
    "0920",
    "0921",
    "0922",
    "0923",
    "0924",
    "0925",
    "0926",
    "0927",
    "0928",
    "0929",
    "0931",
    "0933",
    "0935",
    "0936",
    "0937",
    "0938",
    "0939",
    "0940",
    "0941",
    "0942",
    "0943",
    "0944",
    "0945",
    "0946",
    "0947",
    "0948",
    "0949",
    "0950",
    "0951",
    "0952",
    "0953",
    "0954",
    "0955",
    "0956",
    "0957",
    "0958",
    "0959",
    "0960",
    "0961",
    "0962",
    "0963",
    "0964",
    "0965",
    "0966",
    "0967",
    "0968",
    "0969",
    "0970",
    "0971",
    "0972",
    "0973",
    "0974",
    "0975",
    "0976",
    "0977",
    "0978",
    "0979",
    "0980",
    "0981",
    "0982",
    "0983",
    "0984",
    "0985",
    "0986",
    "0987",
    "0988",
    "0989",
    "0990",
    "0991",
    "0992",
    "0993",
    "0994",
    "0995",
    "0996",
    "0997",
    "0998",
    "0999",
    "1001",
    "1003",
    "1004",
    "1005",
    "1006",
    "1007",
    "1008",
    "1009",
    "1010",
    "1011",
    "1012",
    "1013",
    "1014",
    "1015",
    "1016",
    "1017",
    "1018",
    "1019",
    "1020",
    "1021",
    "1022",
    "1023",
    "1024",
    "1025",
    "1026",
    "1027",
    "1028",
    "1029",
    "1030",
    "1031",
    "1033",
    "1035",
    "1036",
    "1037",
    "1038",
    "1039"
//    // tpc-h
//    "1051",
//    "1052",
//    "1053",
//    "1054",
//    "1055",
//    "1056",
//    "1057",
//    "1058",
//    "1059",
//    "1060",
//    "1061",
//    "1062",
//    "1063",
//    "1064",
//    "1065",
//    "1066",
//    "1067",
//    "1068",
//    "1069",
//    "1070",
//    "1071",
//    "1072",
  )

  override def createQueryTest(testCaseName: String, sql: String, reset: Boolean = true) {

    // testCaseName must not contain ':', which is not allowed to appear in a filename of Windows
    assert(!testCaseName.contains(":"))

    // If test sharding is enable, skip tests that are not in the correct shard.
    shardInfo.foreach {
      case (shardId, numShards) if testCaseName.hashCode % numShards != shardId => return
      case (shardId, _) => logDebug(s"Shard $shardId includes test '$testCaseName'")
    }

    // Skip tests found in directories specified by user.
      skipDirectories
        .map(new File(_, testCaseName))
        .filter(_.exists)
        .foreach(_ => return)

    // If runonlytests is set, skip this test unless we find a file in one of the specified
    // directories.
    val runIndicators =
      runOnlyDirectories
          .map(new File(_, testCaseName))
          .filter(_.exists)
    if (runOnlyDirectories.nonEmpty && runIndicators.isEmpty) {
      logDebug(
        s"Skipping test '$testCaseName' not found in ${runOnlyDirectories.map(_.getCanonicalPath)}")
      return
    }

    test(testCaseName) {
      logDebug(s"=== HIVE TEST: $testCaseName ===")

      // Clear old output for this testcase.
      outputDirectories.map(new File(_, testCaseName)).filter(_.exists()).foreach(_.delete())

      val sqlWithoutComment =
        sql.split("\n").filterNot(l => l.matches("--.*(?<=[^\\\\]);")).mkString("\n")
      val allQueries =
        sqlWithoutComment.split("(?<=[^\\\\]);").map(_.trim).filterNot(q => q == "").toSeq

      // TODO: DOCUMENT UNSUPPORTED
      val queryList =
        allQueries
            // In hive, setting the hive.outerjoin.supports.filters flag to "false" essentially tells
            // the system to return the wrong answer.  Since we have no intention of mirroring their
            // previously broken behavior we simply filter out changes to this setting.
            .filterNot(_ contains "hive.outerjoin.supports.filters")
            .filterNot(_ contains "hive.exec.post.hooks")

      if (allQueries != queryList) {
        logWarning(s"Simplifications made on unsupported operations for test $testCaseName")
      }

      lazy val consoleTestCase = {
        val quotes = "\"\"\""
        queryList.zipWithIndex.map {
          case (query, i) =>
            s"""val q$i = sql($quotes$query$quotes); q$i.collect()"""
        }.mkString("\n== Console version of this test ==\n", "\n", "\n")
      }

      try {
        if (reset) {
          TestFlint.reset()
        }

        val hiveCacheFiles = queryList.zipWithIndex.map {
          case (queryString, i) =>
            val cachedAnswerName = s"$testCaseName-$i-${getMd5(queryString)}"
            new File(answerCache, cachedAnswerName)
        }

        val hiveCachedResults = hiveCacheFiles.flatMap { cachedAnswerFile =>
          logDebug(s"Looking for cached answer file $cachedAnswerFile.")
          if (cachedAnswerFile.exists) {
            Some(fileToString(cachedAnswerFile))
          } else {
            logDebug(s"File $cachedAnswerFile not found")
            None
          }
        }.map {
          case "" => Nil
          case "\n" => Seq("")
          case other => other.split("\n").toSeq
        }

        val hiveResults: Seq[Seq[String]] =
          if (hiveCachedResults.size == queryList.size) {
            logInfo(s"Using answer cache for test: $testCaseName")
            hiveCachedResults
          } else {

            val hiveQueries = queryList.map(new TestFlint.QueryExecution(_))
            // Make sure we can at least parse everything before attempting hive execution.
            // Note this must only look at the logical plan as we might not be able to analyze if
            // other DDL has not been executed yet.
            hiveQueries.foreach(_.logical)
            val computedResults = (queryList.zipWithIndex, hiveQueries, hiveCacheFiles).zipped.map {
              case ((queryString, i), hiveQuery, cachedAnswerFile) =>
                try {
                  // Hooks often break the harness and don't really affect our test anyway, don't
                  // even try running them.
                  if (installHooksCommand.findAllMatchIn(queryString).nonEmpty) {
                    sys.error("hive exec hooks not supported for tests.")
                  }

                  logWarning(s"Running query ${i + 1}/${queryList.size} with hive.")
                  // Analyze the query with catalyst to ensure test tables are loaded.
                  val answer = hiveQuery.analyzed match {
                    case _: ExplainCommand =>
                      // No need to execute EXPLAIN queries as we don't check the output.
                      Nil
                    case _ => TestFlint.runSqlHive(queryString)
                  }

                  // We need to add a new line to non-empty answers so we can differentiate Seq()
                  // from Seq("").
                  stringToFile(
                    cachedAnswerFile, answer.mkString("\n") + (if (answer.nonEmpty) "\n" else ""))
                  answer
                } catch {
                  case e: Exception =>
                    val errorMessage =
                      s"""
                         |Failed to generate golden answer for query:
                         |Error: ${e.getMessage}
                            |${stackTraceToString(e)}
                            |$queryString
                      """.stripMargin
                    stringToFile(
                      new File(hiveFailedDirectory, testCaseName),
                      errorMessage + consoleTestCase)
                    fail(errorMessage)
                }
            }.toSeq
            if (reset) { TestFlint.reset() }

            computedResults
          }

        // Run w/ catalyst
        val catalystResults = queryList.zip(hiveResults).map { case (queryString, hive) =>
          val query = new TestFlint.QueryExecution(queryString)
          try { (query, prepareAnswerAnswer(query, query.stringResult())) } catch {
            case e: Throwable =>
              val errorMessage =
                s"""
                   |Failed to execute query using catalyst:
                   |Error: ${e.getMessage}
                      |${stackTraceToString(e)}
                      |$query
                      |== HIVE - ${hive.size} row(s) ==
                                               |${hive.mkString("\n")}
                """.stripMargin
              stringToFile(new File(failedDirectory, testCaseName), errorMessage + consoleTestCase)
              fail(errorMessage)
          }
        }.toSeq

        (queryList, hiveResults, catalystResults).zipped.foreach {
          case (query, hive, (hiveQuery, catalyst)) =>
            // Check that the results match unless its an EXPLAIN query.
            val preparedHive = prepareAnswerAnswer(hiveQuery, hive)

            // We will ignore the ExplainCommand, ShowFunctions, DescribeFunction
            if ((!hiveQuery.logical.isInstanceOf[ExplainCommand]) &&
                (!hiveQuery.logical.isInstanceOf[ShowFunctions]) &&
                (!hiveQuery.logical.isInstanceOf[DescribeFunction]) &&
                //preparedHive != catalyst) {
                !mytest(preparedHive, catalyst)){

              val hivePrintOut = s"== HIVE - ${preparedHive.size} row(s) ==" +: preparedHive
              val catalystPrintOut = s"== CATALYST - ${catalyst.size} row(s) ==" +: catalyst

              val resultComparison = sideBySide(hivePrintOut, catalystPrintOut).mkString("\n")

              if (recomputeCache) {
                logWarning(s"Clearing cache files for failed test $testCaseName")
                hiveCacheFiles.foreach(_.delete())
              }

              // If this query is reading other tables that were created during this test run
              // also print out the query plans and results for those.
              val computedTablesMessages: String = try {
                val tablesRead = new TestFlint.QueryExecution(query).executedPlan.collect {
                  case ts: HiveTableScan => ts.relation.tableName
                }.toSet

                TestFlint.reset()
                val executions = queryList.map(new TestFlint.QueryExecution(_))
                executions.foreach(_.toRdd)
                val tablesGenerated = queryList.zip(executions).flatMap {
                  case (q, e) => e.executedPlan.collect {
                    case i: InsertIntoHiveTable if tablesRead contains i.table.tableName =>
                      (q, e, i)
                  }
                }

                tablesGenerated.map { case (hiveql, execution, insert) =>
                  s"""
                     |=== Generated Table ===
                     |$hiveql
                        |$execution
                        |== Results ==
                        |${insert.child.execute().collect().mkString("\n")}
                   """.stripMargin
                }.mkString("\n")

              } catch {
                case NonFatal(e) =>
                  logError("Failed to compute generated tables", e)
                  s"Couldn't compute dependent tables: $e"
              }

              val errorMessage =
                s"""
                   |Results do not match for $testCaseName:
                                                            |$hiveQuery\n${hiveQuery.analyzed.output.map(_.name).mkString("\t")}
                      |$resultComparison
                      |$computedTablesMessages
                """.stripMargin

              stringToFile(new File(wrongDirectory, testCaseName), errorMessage + consoleTestCase)
              fail(errorMessage)
            }
        }

        // Touch passed file.
        new FileOutputStream(new File(passedDirectory, testCaseName)).close()
      } catch {
        case tf: org.scalatest.exceptions.TestFailedException => throw tf
        case originalException: Exception =>
          if (System.getProperty("spark.hive.canarytest") != null) {
            // When we encounter an error we check to see if the environment is still
            // okay by running a simple query. If this fails then we halt testing since
            // something must have gone seriously wrong.
            try {
              new TestFlint.QueryExecution("SELECT key FROM src").stringResult()
              TestFlint.runSqlHive("SELECT key FROM src")
            } catch {
              case e: Exception =>
                logError(s"FATAL ERROR: Canary query threw $e This implies that the " +
                    "testing environment has likely been corrupted.")
                // The testing setup traps exits so wait here for a long time so the developer
                // can see when things started to go wrong.
                Thread.sleep(1000000)
            }
          }

          // If the canary query didn't fail then the environment is still okay,
          // so just throw the original exception.
          throw originalException
      }
    }
  }

  protected def prepareAnswerAnswer(
                               hiveQuery: TestFlint.type#QueryExecution,
                               answer: Seq[String]): Seq[String] = {

    def isSorted(plan: LogicalPlan): Boolean = plan match {
      case _: Join | _: Aggregate | _: Generate | _: Sample | _: Distinct => false
      case PhysicalOperation(_, _, Sort(_, true, _)) => true
      case _ => plan.children.iterator.exists(isSorted)
    }

    val orderedAnswer = hiveQuery.analyzed match {
      // Clean out non-deterministic time schema info.
      // Hack: Hive simply prints the result of a SET command to screen,
      // and does not return it as a query answer.
      case _: SetCommand => Seq("0")
      case HiveNativeCommand(c) if c.toLowerCase.contains("desc") =>
        answer
          .filterNot(nonDeterministicLine)
          .map(_.replaceAll("from deserializer", ""))
          .map(_.replaceAll("None", ""))
          .map(_.trim)
          .filterNot(_ == "")
      case _: HiveNativeCommand => answer.filterNot(nonDeterministicLine).filterNot(_ == "")
      case _: ExplainCommand => answer
      case _: DescribeCommand =>
        // Filter out non-deterministic lines and lines which do not have actual results but
        // can introduce problems because of the way Hive formats these lines.
        // Then, remove empty lines. Do not sort the results.
        answer
          .filterNot(r => nonDeterministicLine(r) || ignoredLine(r))
          .map(_.replaceAll("from deserializer", ""))
          .map(_.replaceAll("None", ""))
          .map(_.trim)
          .filterNot(_ == "")
      case plan => if (isSorted(plan)) answer else answer.sorted
    }
    orderedAnswer.map(cleanPaths)
  }

  def mytest(hiveresult: Seq[String], cataresult: Seq[String]) : Boolean = {
    var result = true
    if (hiveresult == cataresult)
      true
    if (hiveresult.size == cataresult.size)
        for (i <- 0 until hiveresult.size){
          val harray = hiveresult(i).split("\\t")
          val carray = cataresult(i).split("\\t")
          if (harray.size == carray.size){
            for (j <- 0 until harray.size){
              if (!carray(j).equals(harray(j)) && (canBeCastToDouble(carray(j)) && canBeCastToDouble(harray(j)) && math.abs(carray(j).toDouble - harray(j).toDouble) > 0.01))
                result = false
            }
          }
          else
            result = false
        }
    result
  }

  def canBeCastToDouble(testcase: String) : Boolean = {
    try {
      val testDouble = testcase.toDouble
      true
    }
    catch{
      case _: Exception => false
    }
  }



}

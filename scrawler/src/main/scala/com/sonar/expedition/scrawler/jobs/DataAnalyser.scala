/*
package com.sonar.expedition.scrawler.jobs

import com.sonar.expedition.scrawler.apis.APICalls
import com.sonar.expedition.scrawler.util._
import com.twitter.scalding._
import com.sonar.expedition.scrawler.util.CommonFunctions._
import com.sonar.expedition.scrawler.pipes._
import scala.util.matching.Regex
import cascading.pipe.joiner._
import com.twitter.scalding.TextLine
import cascading.tuple.Fields
import com.lambdaworks.jacks._
import tools.nsc.io.Streamable.Bytes
import ch.hsr.geohash.GeoHash


/*

      com.sonar.expedition.scrawler.jobs.DataAnalyser --hdfs --serviceProfileData "/tmp/serviceProfileData.txt" --friendData "/tmp/friendData.txt"  --checkinData "/tmp/checkinDatatest.txt" --placesData "/tmp/places_dump_US.geojson.txt"
        --output "/tmp/dataAnalyseroutput.txt" --occupationCodetsv "/tmp/occupationCodetsv.txt" --male "/tmp/male.txt" --female "/tmp/female.txt" --occupationCodeTsvOutpipe "/tmp/occupationCodeTsvOutpipe"
        --genderdataOutpipe "/tmp/genderdataOutpipe" --bayestrainingmodel "/tmp/bayestrainingmodel" --outputclassify "/tmp/jobclassified" --genderoutput "/tmp/genderoutput"  --outputjobtypes "/tmp/outputjobtypes"  --jobtype "3"
         --profileCount "/tmp/profileCount.txt" --serviceCount "/tmp/serviceCount.txt" --geoCount "/tmp/geoCount.txt
         --debug1 "/tmp/debug1" --debug2 "/tmp/debug2"  --debug3 "/tmp/debug3" --debug4 "/tmp/debug4" --debug5 "/tmp/debug5" --debug6 "/tmp/debug6"  --debug7 "/tmp/debug7" --debug8 "/tmp/debug8" --debug8 "/tmp/debug9"
         --geohashsectorsize "20"


     --jobtype "3"
        1 for only no gender and job classification
        2 for only gender
        3 for only job classification
        4 for both

        com.sonar.expedition.scrawler.jobs.DataAnalyser --hdfs --serviceProfileData "/tmp/serviceProfileData.txt" --friendData "/tmp/friendData.txt" --checkinData "/tmp/checkinDatatest.txt" --placesData "/tmp/places_dump_US.geojson.txt" --output "/tmp/dataAnalyseroutput.txt" --occupationCodetsv "/tmp/occupationCodetsv.txt" --occupationCodeTsvOutpipe "/tmp/occupationCodeTsvOutpipe" --genderdataOutpipe "/tmp/genderdataOutpipe" --bayestrainingmodel "/tmp/bayestrainingmodel" --outputclassify "/tmp/jobclassified" --genderoutput "/tmp/genderoutput"  --outputjobtypes "/tmp/outputjobtypes"  --jobtype "3"  --profileCount "/tmp/profileCount.txt" --serviceCount "/tmp/serviceCount.txt" --geoCount "/tmp/geoCount.txt"

        com.sonar.expedition.scrawler.jobs.DataAnalyser --hdfs --serviceProfileData "/tmp/serviceProfileData.txt" --friendData "/tmp/friendData.txt" --checkinData "/tmp/checkinDatatest.txt" --placesData "/tmp/places_dump_US.geojson.txt" --output "/tmp/dataAnalyseroutput.txt" --occupationCodetsv "/tmp/occupationCodetsv.txt" --occupationCodeTsvOutpipe "/tmp/occupationCodeTsvOutpipe" --genderdataOutpipe "/tmp/genderdataOutpipe" --bayestrainingmodel "/tmp/bayestrainingmodel" --outputclassify "/tmp/jobclassified" --genderoutput "/tmp/genderoutput"  --outputjobtypes "/tmp/outputjobtypes"  --jobtype "4"  --profileCount "/tmp/profileCount.txt" --serviceCount "/tmp/serviceCount.txt" --geoCount "/tmp/geoCount.txt" --trainedseqmodel  "/tmp/trainedseqmodel" --debug1 "/tmp/debug1" --debug2 "/tmp/debug2"  --debug3 "/tmp/debug3" --debug4 "/tmp/debug4" --debug5 "/tmp/debug5" --debug6 "/tmp/debug6"  --debug7 "/tmp/debug7" --debug8 "/tmp/debug8" --debug9 "/tmp/debug9" --geoHash "/tmp/geoHash" --geohashsectorsize "1000" --groupcountry "/tmp/groupcountry" --groupworktitle "/tmp/groupworktitle"  --groupcity "/tmp/groupcity"
 */


class DataAnalyser(args: Args) extends Job(args) {

    val inputData = args("serviceProfileData")
    val finp = args("friendData")
    val chkininputData = args("checkinData")
    val jobOutput = args("output")
    val jobOutputclasslabel = args("outputclassify")
    val placesData = args("placesData")
    val bayestrainingmodel = args("bayestrainingmodel")
    val genderoutput = args("genderoutput")
    val profileCount = args("profileCount")
    val serviceCount = args("serviceCount")
    val geoCount = args("geoCount")
    val jobtypeTorun = args("jobtype")
    val trainedseqmodel = args("trainedseqmodel")
    val geohashsectorsize = args.getOrElse("geohashsectorsize", "20").toInt
    val groupworktitle = args("groupworktitle")
    val groupcountry = args("groupcountry")
    val groupcity = args("groupcity")
    /*
    val debug1 = args.getOrElse("debug1","s3n://scrawler/debug1")
    val debug3 = args.getOrElse("debug2","s3n://scrawler/debug3")
    val debug4 = args.getOrElse("debug2","s3n://scrawler/debug4")
    val debug1 = args.getOrElse("debug1","/tmp/debug1")
    val debug3 = args.getOrElse("debug2","/tmp/debug3")
    val debug4 = args.getOrElse("debug2","/tmp/debug4")
    */
    // /{\"service_type\":\"facebook\",\"user_id\":\"565599586\",\"url\":\"http://www\.facebook\.com/steve153\",\"aliases\":{\"username\":\"steve153\",\"phone\":null,\"email\":null,\"facebook\":null,\"twitter\":null},\"bio\":\"Partner, Chief Creative Officer at Mirrorball\",\"hometown\":\"Bronxville, New York\",\"location\":\"New York, New York\",\"full_name\":\"Stephen Papageorge\",\"raw_profile\":null,\"work\":[{\"key\":\"114849121862080\",\"company_name\":\"Mirrorball\",\"title\":\"Partner, Chief Creative Officer\",\"summary\":null,\"current\":true},{\"key\":\"143906941286\",\"company_name\":\"Mirrorball\",\"title\":null,\"summary\":null,\"current\":true},{\"key\":\"115347011809220\",\"company_name\":\"Harley Davidson Motor Company\",\"title\":null,\"summary\":null,\"current\":true}],\"education\":[{\"id\":110189772337852,\"key\":\"110189772337852\",\"year\":null,\"field_of_study\":null,\"degree\":null,\"school_name\":\"Bronxville High School\",\"activities\":null},{\"id\":20697868961,\"key\":\"20697868961\",\"year\":\"1990\",\"field_of_study\":null,\"degree\":null,\"school_name\":\"Boston University\",\"activities\":null}],\"like\":[{\"key\":\"254276857934828\",\"header\":\"Hifi House\",\"category\":null},{\"key\":\"111856595508107\",\"header\":\"Sabre Yachts\",\"category\":null},{\"key\":\"350462218332281\",\"header\":\"Ivan Kane's Royal Jelly Burlesque Nightclub\",\"category\":null},{\"key\":\"149819661715632\",\"header\":\"Madagascar Institute\",\"category\":null},{\"key\":\"96943760335\",\"header\":\"Nikki Sixx\",\"category\":null},{\"key\":\"231039546868\",\"header\":\"Mykonos Blu Grecotel Exclusive Resort\",\"category\":null},{\"key\":\"270497689660929\",\"header\":\"Aureus Yachts\",\"category\":null},{\"key\":\"136946756356288\",\"header\":\"PlanetSolar\",\"category\":null},{\"key\":\"170822346291114\",\"header\":\"Mechatronics & Electrical power Engineering\",\"category\":null},{\"key\":\"143545855670318\",\"header\":\"Andy Kessler\",\"category\":null},{\"key\":\"37312746513\",\"header\":\"Neil Diamond\",\"category\":null},{\"key\":\"146959058654370\",\"header\":\"Kogeto\",\"category\":null},{\"key\":\"190748727692178\",\"header\":\"Empire Harley-Davidson\",\"category\":null},{\"key\":\"132569606820110\",\"header\":\"Good Stuff Eatery\",\"category\":null},{\"key\":\"261752393840398\",\"header\":\"Dana Tanamachi\",\"category\":null},{\"key\":\"106185516116010\",\"header\":\"I Support Neil Diamond for the Rock n Roll Hall of Fame\",\"category\":null},{\"key\":\"6836637082\",\"header\":\"Art in the Age\",\"category\":null},{\"key\":\"139635306094\",\"header\":\"TEDxEast\",\"category\":null},{\"key\":\"212719272125229\",\"header\":\"Marc Forgione\",\"category\":null},{\"key\":\"119473960614\",\"header\":\"Malcolm Fontier\",\"category\":null},{\"key\":\"256828326943\",\"header\":\"The Aurelia Group\",\"category\":null},{\"key\":\"75334965943\",\"header\":\"THE CULTURE OF ME\",\"category\":null},{\"key\":\"111170002310044\",\"header\":\"The Delancey\",\"category\":null},{\"key\":\"18797601233\",\"header\":\"Social Media Today\",\"category\":null},{\"key\":\"122143042319\",\"header\":\"ImageThink - Graphic Recording\",\"category\":null},{\"key\":\"156742961051018\",\"header\":\"Core77 Design Awards\",\"category\":null},{\"key\":\"126944527336327\",\"header\":\"BMW Motorcycles\",\"category\":null},{\"key\":\"63739675517\",\"header\":\"Triumph Motorcycles\",\"category\":null},{\"key\":\"51504397489\",\"header\":\"Ducati North America\",\"category\":null},{\"key\":\"131110706662\",\"header\":\"Purple Pam\",\"category\":null},{\"key\":\"10664530778\",\"header\":\"Rolling Stone\",\"category\":null},{\"key\":\"255705494521817\",\"header\":\"The Black Keys at Revel Ovation Hall on May 19, 2012\",\"category\":null},{\"key\":\"110218607088\",\"header\":\"Knitting Factory Brooklyn\",\"category\":null},{\"key\":\"27808104606\",\"header\":\"IronWorks Magazine\",\"category\":null},{\"key\":\"18582762347\",\"header\":\"DENNIS HOPPER\",\"category\":null},{\"key\":\"16031924681\",\"header\":\"VINTAGE MOTORCYCLES\",\"category\":null},{\"key\":\"29971967884\",\"header\":\"Vintage Harley-Davidson Motorcycles\",\"category\":null},{\"key\":\"33301371231\",\"header\":\"REBELS, OUTSIDERS & OUTLAWS\",\"category\":null},{\"key\":\"152774364748531\",\"header\":\"Evel Knievel\",\"category\":null},{\"key\":\"115384328477363\",\"header\":\"The Creators Project\",\"category\":null},{\"key\":\"112154178795952\",\"header\":\"Rolleiflex\",\"category\":null},{\"key\":\"135855763099304\",\"header\":\"A76 PRODUCTIONS\",\"category\":null},{\"key\":\"51583012511\",\"header\":\"Biltwell Inc\.\",\"category\":null},{\"key\":\"353851465130\",\"header\":\"SpaceX\",\"category\":null},{\"key\":\"126960357373633\",\"header\":\"Leslie Van Stelten Photography\",\"category\":null},{\"key\":\"270212045657\",\"header\":\"Wimp\.com\",\"category\":null},{\"key\":\"143528075748871\",\"header\":\"mdolla\",\"category\":null},{\"key\":\"6815651902\",\"header\":\"The FADER\",\"category\":null},{\"key\":\"19425461584\",\"header\":\"Likeable Media\",\"category\":null},{\"key\":\"257044484325795\",\"header\":\"Focus Forward\",\"category\":null},{\"key\":\"156577107797007\",\"header\":\"Arts Not Fair\",\"category\":null},{\"key\":\"81405870417\",\"header\":\"technabob\",\"category\":null},{\"key\":\"360747867770\",\"header\":\"Terry Hope Romero, Vegan Nerdista Cookbookista\",\"category\":null},{\"key\":\"12525692211\",\"header\":\"Hasselblad\",\"category\":null},{\"key\":\"228441593836570\",\"header\":\"Zimmerman Advertising\",\"category\":null},{\"key\":\"165947383480263\",\"header\":\"Fenway Cafe\",\"category\":null},{\"key\":\"375536199141474\",\"header\":\"Urbanspace Interiors\",\"category\":null},{\"key\":\"55152637380\",\"header\":\"FontSpace\",\"category\":null},{\"key\":\"143736655646402\",\"header\":\"Bikers by Biker Planet\",\"category\":null},{\"key\":\"106994762713409\",\"header\":\"The Greatist\",\"category\":null},{\"key\":\"217512775360\",\"header\":\"NYC: The Official Guide\",\"category\":null},{\"key\":\"148256118536035\",\"header\":\"DonationTo\",\"category\":null},{\"key\":\"203819169788\",\"header\":\"Social Media Week\",\"category\":null},{\"key\":\"208491679242606\",\"header\":\"Dumptruck\",\"category\":null},{\"key\":\"337061459425\",\"header\":\"Klip Collective\",\"category\":null},{\"key\":\"263430107029119\",\"header\":\"Hinterland Erie Street Gastropub\",\"category\":null},{\"key\":\"351078719179\",\"header\":\"Dudley's\",\"category\":null},{\"key\":\"97390772344\",\"header\":\"The Driskill\",\"category\":null},{\"key\":\"118893711530537\",\"header\":\"Revel\",\"category\":null},{\"key\":\"17432988290\",\"header\":\"Sons of Anarchy\",\"category\":null},{\"key\":\"153278754738777\",\"header\":\"Climate Reality\",\"category\":null},{\"key\":\"136799989684142\",\"header\":\"BikeweekNews\.blog\",\"category\":null},{\"key\":\"34361254896\",\"header\":\"Scene 360 Magazine\",\"category\":null},{\"key\":\"146249933778\",\"header\":\"stephie coplan\",\"category\":null},{\"key\":\"108170975877442\",\"header\":\"Photography\",\"category\":null},{\"key\":\"199573883462879\",\"header\":\"Lettermon\",\"category\":null},{\"key\":\"200308533320720\",\"header\":\"Colossal\",\"category\":null},{\"key\":\"31325960871\",\"header\":\"Richard Branson\",\"category\":null},{\"key\":\"5718758966\",\"header\":\"Gizmodo\",\"category\":null},{\"key\":\"111787580355\",\"header\":\"Motorcycle Superstore\",\"category\":null},{\"key\":\"9181619833\",\"header\":\"International CES\",\"category\":null},{\"key\":\"212228975264\",\"header\":\"David Merlini\",\"category\":null},{\"key\":\"20281766647\",\"header\":\"Coachella\",\"category\":null},{\"key\":\"39283586422\",\"header\":\"Junk Food Clothing\",\"category\":null},{\"key\":\"138814655195\",\"header\":\"Veselka :: Bece??a\",\"category\":null},{\"key\":\"19196585990\",\"header\":\"Film Movement\",\"category\":null},{\"key\":\"212641675376\",\"header\":\"im not right in the head\.com\",\"category\":null},{\"key\":\"139816059464705\",\"header\":\"09 R\.I\.P/The Desert Song\.m4a\",\"category\":null},{\"key\":\"149225605096289\",\"header\":\"Karen Curious\",\"category\":null},{\"key\":\"273775362655821\",\"header\":\"JET + LVS Productions\",\"category\":null},{\"key\":\"13587987060\",\"header\":\"Malcolm Gladwell\",\"category\":null},{\"key\":\"133098555222\",\"header\":\"JETROPHELIA\",\"category\":null},{\"key\":\"233893466620992\",\"header\":\"Charlotte Hounds\",\"category\":null},{\"key\":\"96311975576\",\"header\":\"Brightline Interactive\",\"category\":null},{\"key\":\"262682050446583\",\"header\":\"World Hellenic Society\",\"category\":null},{\"key\":\"205344452828349\",\"header\":\"George Takei\",\"category\":null},{\"key\":\"100939993327713\",\"header\":\"Newcastle Foosball League\",\"category\":null},{\"key\":\"201477613214009\",\"header\":\"The Football Factory at Legends\",\"category\":null},{\"key\":\"129726120414880\",\"header\":\"Newcastle Region's Markets, Festivals and Fairs\",\"category\":null},{\"key\":\"204116409603953\",\"header\":\"Events in Newcastle\",\"category\":null},{\"key\":\"18870062776\",\"header\":\"Creative Commons\",\"category\":null},{\"key\":\"149730265360\",\"header\":\"Picciones'\",\"category\":null},{\"key\":\"124119160931672\",\"header\":\"Newcastle United\",\"category\":null},{\"key\":\"212615362103982\",\"header\":\"Newcastle Brown Ale\",\"category\":null},{\"key\":\"82643606520\",\"header\":\"FAGE Total Greek Yogurt\",\"category\":null},{\"key\":\"14062458815\",\"header\":\"Seamless\",\"category\":null},{\"key\":\"49430399644\",\"header\":\"Foodswings\",\"category\":null},{\"key\":\"7616161690\",\"header\":\"Wacom Americas\",\"category\":null},{\"key\":\"62437291716\",\"header\":\"Windstar Cruises\",\"category\":null},{\"key\":\"165946580105995\",\"header\":\"Monemvasia Winery\",\"category\":null},{\"key\":\"144118862284523\",\"header\":\"The Las Vegas Halloween Parade\",\"category\":null},{\"key\":\"91800498227\",\"header\":\"Visit Las Vegas\",\"category\":null},{\"key\":\"111882778822089\",\"header\":\"Davone\",\"category\":null},{\"key\":\"52193296770\",\"header\":\"Prezi\",\"category\":null},{\"key\":\"118571850968\",\"header\":\"International Poster Gallery\",\"category\":null},{\"key\":\"116551511703911\",\"header\":\"Ecomagination\",\"category\":null},{\"key\":\"138602119541424\",\"header\":\"GE\",\"category\":null},{\"key\":\"5863091683\",\"header\":\"TripAdvisor\",\"category\":null},{\"key\":\"293244287321\",\"header\":\"Thompson LES\",\"category\":null},{\"key\":\"87011584968\",\"header\":\"Thompson Hotels\",\"category\":null},{\"key\":\"390007340357\",\"header\":\"Natural News\",\"category\":null},{\"key\":\"56093345079\",\"header\":\"Plant The Future\",\"category\":null},{\"key\":\"235596193145333\",\"header\":\"laura hanifin photography\",\"category\":null},{\"key\":\"178165565565000\",\"header\":\"SpyderPig\",\"category\":null},{\"key\":\"113769981999374\",\"header\":\"Rainbow Ryders, Inc\. Hot Air Balloon Co\.\",\"category\":null},{\"key\":\"229539953769666\",\"header\":\"Albuquerque balloon fest!\",\"category\":null},{\"key\":\"124221400244\",\"header\":\"Mullen\",\"category\":null},{\"key\":\"119415718023\",\"header\":\"Mike Baker the Bike Maker\",\"category\":null},{\"key\":\"116066535070212\",\"header\":\"Songtrust\",\"category\":null},{\"key\":\"181894861866102\",\"header\":\"MoneyBall\",\"category\":null},{\"key\":\"153774071379194\",\"header\":\"Occupy Wall Street\",\"category\":null},{\"key\":\"126756847074\",\"header\":\"Adweek\",\"category\":null},{\"key\":\"110205140528\",\"header\":\"Plum TV ? Aspen\",\"category\":null},{\"key\":\"113015385391934\",\"header\":\"Plum TV\",\"category\":null},{\"key\":\"227655380608587\",\"header\":\"1950's lounge and vintage everything\",\"category\":null},{\"key\":\"158613407523269\",\"header\":\"Dig Inn Seasonal Market\",\"category\":null},{\"key\":\"55855811556\",\"header\":\"Falcon Motorcycles\",\"category\":null},{\"key\":\"158776627523351\",\"header\":\"Pipeburn\",\"category\":null},{\"key\":\"41121168667\",\"header\":\"Post Punk Kitchen\",\"category\":null},{\"key\":\"112182808793631\",\"header\":\"Post Punk Kitchen\",\"category\":null},{\"key\":\"105701639464659\",\"header\":\"Isa Chandra Moskowitz\",\"category\":null},{\"key\":\"202825023077692\",\"header\":\"Beast\",\"category\":null},{\"key\":\"168404366545638\",\"header\":\"La Maltese Estate Santorini, Relais & Chateaux\",\"category\":null},{\"key\":\"34374480881\",\"header\":\"The Adventure School\",\"category\":null},{\"key\":\"74137674486\",\"header\":\"VaynerMedia\",\"category\":null},{\"key\":\"213808565333478\",\"header\":\"W&W Cycles AG\",\"category\":null},{\"key\":\"53022517072\",\"header\":\"CruiserCustomizing\.com\",\"category\":null},{\"key\":\"63630623610\",\"header\":\"J&P Cycles\",\"category\":null},{\"key\":\"113529011990795\",\"header\":\"Steve Jobs\",\"category\":null},{\"key\":\"9465008123\",\"header\":\"Amazon\.com\",\"category\":null},{\"key\":\"106881602693380\",\"header\":\"Artupdate\",\"category\":null},{\"key\":\"18533033589\",\"header\":\"Chuck Close\",\"category\":null},{\"key\":\"180341268753\",\"header\":\"Vita Coco Coconut Water\",\"category\":null},{\"key\":\"176120179069074\",\"header\":\"Munitio\",\"category\":null},{\"key\":\"97470139355\",\"header\":\"Larchmont, New York\",\"category\":null},{\"key\":\"175082549212927\",\"header\":\"The Daily Larchmont\",\"category\":null},{\"key\":\"216188691732676\",\"header\":\"Hpnotiq Trinidad & Tobago\",\"category\":null},{\"key\":\"112281368823423\",\"header\":\"ARMA Energy\",\"category\":null},{\"key\":\"6029094653\",\"header\":\"GrubHub\.com\",\"category\":null},{\"key\":\"176259485728272\",\"header\":\"The Ivy\",\"category\":null},{\"key\":\"132391040138681\",\"header\":\"Yankee Stadium\",\"category\":null},{\"key\":\"36773816512\",\"header\":\"FLIP burger boutique\",\"category\":null},{\"key\":\"20534666726\",\"header\":\"Food Network\",\"category\":null},{\"key\":\"217106418312141\",\"header\":\"Greenwich Mountain Lion\",\"category\":null},{\"key\":\"196207327063866\",\"header\":\"The Daily Bronxville\",\"category\":null},{\"key\":\"17437686691\",\"header\":\"Squeaky Wheel Media\",\"category\":null},{\"key\":\"172997149423672\",\"header\":\"SwellShark\",\"category\":null},{\"key\":\"329945398711\",\"header\":\"The Viper Vixens\",\"category\":null},{\"key\":\"355724316455\",\"header\":\"DBGB Kitchen and Bar Official Restaurant Page\",\"category\":null},{\"key\":\"189119591099453\",\"header\":\"David Burke Kitchen\",\"category\":null},{\"key\":\"149170741793864\",\"header\":\"Gilt City\",\"category\":null},{\"key\":\"214368275258797\",\"header\":\"One Art Space\",\"category\":null},{\"key\":\"192585854122800\",\"header\":\"Berocca\",\"category\":null},{\"key\":\"99686569869\",\"header\":\"Mohonk Mountain House\",\"category\":null},{\"key\":\"280662395211\",\"header\":\"ThisIsLike\.Com\",\"category\":null},{\"key\":\"162143505613\",\"header\":\"Bully Blends Coffee & Tea Shop\",\"category\":null},{\"key\":\"195563583830731\",\"header\":\"WAVE Seafood Kitchen\",\"category\":null},{\"key\":\"201865225814\",\"header\":\"Roland Sands Design\",\"category\":null},{\"key\":\"134567699941447\",\"header\":\"Camping Andros\",\"category\":null},{\"key\":\"157429890950859\",\"header\":\"Andros , Greece\",\"category\":null},{\"key\":\"108108466413\",\"header\":\"Andros 365\",\"category\":null},{\"key\":\"189304447779677\",\"header\":\"\\\"Vintsi\\\", Andros\",\"category\":null},{\"key\":\"146915015354658\",\"header\":\"Allstate Motorcycle\",\"category\":null},{\"key\":\"114050161948682\",\"header\":\"Reuters\",\"category\":null},{\"key\":\"161275430585313\",\"header\":\"Ameriprise Financial\",\"category\":null},{\"key\":\"138422129506130\",\"header\":\"PhysiPet LLC\",\"category\":null},{\"key\":\"116420718454429\",\"header\":\"Elevation Partners Director and Co-Founder Roger McNamee\",\"category\":null},{\"key\":\"179548858751530\",\"header\":\"Dia De Los Toadies\",\"category\":null},{\"key\":\"117121788357826\",\"header\":\"LeahStunts\",\"category\":null},{\"key\":\"217011484985742\",\"header\":\"Hot Bike\",\"category\":null},{\"key\":\"186660874699788\",\"header\":\"nonBored\.com\",\"category\":null},{\"key\":\"138702996158754\",\"header\":\"Willie G\. Davidson\",\"category\":null},{\"key\":\"275583034348\",\"header\":\"Fab\.com\",\"category\":null},{\"key\":\"28762700577\",\"header\":\"Involvio\",\"category\":null},{\"key\":\"186274234765660\",\"header\":\"Jeanne Darst/Fiction Ruined My Family\",\"category\":null},{\"key\":\"110525815643144\",\"header\":\"Restaurants\",\"category\":null},{\"key\":\"74100576336\",\"header\":\"Facebook Marketing\",\"category\":null},{\"key\":\"7240312795\",\"header\":\"The Motley Fool\",\"category\":null},{\"key\":\"7050739895\",\"header\":\"Style\.com\",\"category\":null},{\"key\":\"77400818166\",\"header\":\"Bee Raw Honey\",\"category\":null},{\"key\":\"96330778028\",\"header\":\"aptsandlofts\.com\",\"category\":null},{\"key\":\"47370597479\",\"header\":\"Stay Thirsty Media, Inc\.\",\"category\":null},{\"key\":\"112373888809455\",\"header\":\"Patr?n Tequila Cocktail Lab\",\"category\":null},{\"key\":\"37469599796\",\"header\":\"Tasting Table\",\"category\":null},{\"key\":\"23006171080\",\"header\":\"Jeremy Piven\",\"category\":null},{\"key\":\"20753979168\",\"header\":\"Design Museum\",\"category\":null},{\"key\":\"175755689129519\",\"header\":\"Tech in Asia\",\"category\":null},{\"key\":\"133724933333831\",\"header\":\"Pee Wee Kirkland\",\"category\":null},{\"key\":\"294606623173\",\"header\":\"Thompson Beverly Hills\",\"category\":null},{\"key\":\"151682344867318\",\"header\":\"Hotel Tonight\",\"category\":null},{\"key\":\"169658789756034\",\"header\":\"Mette Lindberg\",\"category\":null},{\"key\":\"195496453828297\",\"header\":\"Dor? Cr?perie\",\"category\":null},{\"key\":\"132650476760454\",\"header\":\"Breitling\",\"category\":null},{\"key\":\"151130128291375\",\"header\":\"Kasbah Moderne: Fully Restored Vintage Typewriters\",\"category\":null},{\"key\":\"315560631487\",\"header\":\"Bald Eagle Harley-Davidson\",\"category\":null},{\"key\":\"213665552007694\",\"header\":\"Dynamic Productions\",\"category\":null},{\"key\":\"6743974125\",\"header\":\"Newmindspace\",\"category\":null},{\"key\":\"180048600476\",\"header\":\"Cult of Mac\",\"category\":null},{\"key\":\"107799112594867\",\"header\":\"Newmindspace\",\"category\":null},{\"key\":\"55242959462\",\"header\":\"Harley-Davidson Museum\",\"category\":null},{\"key\":\"62238914751\",\"header\":\"Black Hills Harley-Davidson\",\"category\":null},{\"key\":\"129940227023213\",\"header\":\"GrubHub\.com\",\"category\":null},{\"key\":\"112936425387489\",\"header\":\"Music\",\"category\":null},{\"key\":\"124560407568583\",\"header\":\"Soci?t? Perrier M?xico\",\"category\":null},{\"key\":\"160045584441\",\"header\":\"?????? ? ?????????? ??? ???????!!!\",\"category\":null},{\"key\":\"50359598682\",\"header\":\"Saint Bernadette\",\"category\":null},{\"key\":\"199378593410196\",\"header\":\"Telly Savalas Live\",\"category\":null},{\"key\":\"125872900758994\",\"header\":\"Ryan McGinness Studios, Inc\.\",\"category\":null},{\"key\":\"300636014097\",\"header\":\"Stayin' Safe Motorcycle Training\",\"category\":null},{\"key\":\"110206119000400\",\"header\":\"Crab Shell Restaurant\",\"category\":null},{\"key\":\"130803906987713\",\"header\":\"Stamford Harbor Live\",\"category\":null},{\"key\":\"87507802521\",\"header\":\"Sasckya Porto\",\"category\":null},{\"key\":\"210164909006180\",\"header\":\"NY Ink\",\"category\":null},{\"key\":\"180615545310234\",\"header\":\"Sturgis Bike Week - Rally Bike Bus\",\"category\":null},{\"key\":\"113473332003152\",\"header\":\"Like the Spice Gallery\",\"category\":null},{\"key\":\"185098941511488\",\"header\":\"Laconia Bike Week - Rally Bike Bus\",\"category\":null},{\"key\":\"129052553782754\",\"header\":\"Boxman Studios\",\"category\":null},{\"key\":\"147971431887707\",\"header\":\"Guest Of A Guest\",\"category\":null},{\"key\":\"55909863996\",\"header\":\"Wyndham Garden Hotel Manhattan Chelsea\",\"category\":null},{\"key\":\"91662740741\",\"header\":\"Hpnotiq\",\"category\":null},{\"key\":\"194053120879\",\"header\":\"Advertising Age\",\"category\":null},{\"key\":\"59811402250\",\"header\":\"Creativity\",\"category\":null},{\"key\":\"139644369407837\",\"header\":\"Fake Love\",\"category\":null},{\"key\":\"323772930193\",\"header\":\"Marines\",\"category\":null},{\"key\":\"44053938557\",\"header\":\"The U\.S\. Army\",\"category\":null},{\"key\":\"74281347822\",\"header\":\"U\.S\. Navy\",\"category\":null},{\"key\":\"35414584488\",\"header\":\"U\.S\. Navy SEAL & SWCC Page\",\"category\":null},{\"key\":\"203705509669392\",\"header\":\"Princess Beatrice's ridiculous Royal Wedding hat\",\"category\":null},{\"key\":\"221197184561047\",\"header\":\"Dos Equis Feast Of The Brave Taco Truck\",\"category\":null},{\"key\":\"142659939118337\",\"header\":\"Jumbo Stay Offical Fan Page\",\"category\":null},{\"key\":\"158273547547695\",\"header\":\"Cary Audio\",\"category\":null},{\"key\":\"20950654496\",\"header\":\"The Onion\",\"category\":null},{\"key\":\"115496826818\",\"header\":\"Pawn Stars on History\",\"category\":null},{\"key\":\"14352220141\",\"header\":\"30 Rock\",\"category\":null},{\"key\":\"23680604925\",\"header\":\"Mus?e du Louvre\",\"category\":null},{\"key\":\"129918700359328\",\"header\":\"American Express OPEN\",\"category\":null},{\"key\":\"8234784769\",\"header\":\"Social Times\",\"category\":null},{\"key\":\"499364595720\",\"header\":\"Hidden Los Angeles - The Foodie Page!!!\",\"category\":null},{\"key\":\"135902132745\",\"header\":\"Buddha-Bar\",\"category\":null},{\"key\":\"149071961796732\",\"header\":\"Worlds Best Bars\",\"category\":null},{\"key\":\"215105290242\",\"header\":\"The Intoxicologist\",\"category\":null},{\"key\":\"148434701859001\",\"header\":\"Moschini Productions\",\"category\":null},{\"key\":\"37408316105\",\"header\":\"Whale Wars\",\"category\":null},{\"key\":\"114849121862080\",\"header\":\"Mirrorball\",\"category\":null},{\"key\":\"195995133771387\",\"header\":\"*as of yet, untitlted\.\",\"category\":null},{\"key\":\"6028461107\",\"header\":\"Moby\",\"category\":null},{\"key\":\"389238868661\",\"header\":\"Deep Eddy Vodka\",\"category\":null},{\"key\":\"55951368875\",\"header\":\"Newport Waterfront Events\",\"category\":null},{\"key\":\"171155222929478\",\"header\":\"Tanya Fischer\",\"category\":null},{\"key\":\"195292733833394\",\"header\":\"Greenwich Boat Show\",\"category\":null},{\"key\":\"162012100523154\",\"header\":\"Pledging My Time, by Peter Parcek\",\"category\":null},{\"key\":\"10476912572\",\"header\":\"The Asteroids Galaxy Tour\",\"category\":null},{\"key\":\"46359330109\",\"header\":\"Ozomatli\",\"category\":null},{\"key\":\"79412962324\",\"header\":\"Neon Trees\",\"category\":null},{\"key\":\"6185812851\",\"header\":\"American Express\",\"category\":null},{\"key\":\"128212080545716\",\"header\":\"TheVisualMD\.com\",\"category\":null},{\"key\":\"145193425503642\",\"header\":\"Metaxa\",\"category\":null},{\"key\":\"65710715696\",\"header\":\"Patsy's Italian Restaurant\",\"category\":null},{\"key\":\"120559927959167\",\"header\":\"Patsys Italian Restaurant\",\"category\":null},{\"key\":\"7568536355\",\"header\":\"Lifehacker\",\"category\":null},{\"key\":\"185706851443110\",\"header\":\"Bartaco Port Chester\",\"category\":null},{\"key\":\"145663485496045\",\"header\":\"Charlie Sheen\",\"category\":null},{\"key\":\"132853273433650\",\"header\":\"Enchantment\",\"category\":null},{\"key\":\"108905269168364\",\"header\":\"Conan O'Brien Presents: Team Coco\",\"category\":null},{\"key\":\"131247850118\",\"header\":\"YouSendIt\",\"category\":null},{\"key\":\"6363207806\",\"header\":\"CollegeHumor\",\"category\":null},{\"key\":\"175052472533050\",\"header\":\"RootedGrass Music\",\"category\":null},{\"key\":\"121520047789\",\"header\":\"Tough Mudder\",\"category\":null},{\"key\":\"18050317849\",\"header\":\"Brough Superior Motorcycles\",\"category\":null},{\"key\":\"177290528982630\",\"header\":\"Spaceb" .


    val data = (TextLine(inputData).read.project('line).flatMap(('line) ->('id, 'serviceType, 'jsondata)) {
        line: String => {
            line match {
                    case ServiceProfileExtractLine(userProfileId, serviceType, json) => Some((userProfileId, serviceType, json))
                    case _ => None
            }
        }
    }).project(('id, 'serviceType, 'jsondata))

    //val profuniqcount = data.unique('id).groupAll{_.size}.write(TextLine(debug3))


    val dtoProfileGetPipe = new DTOProfileInfoPipe(args)
    val employerGroupedServiceProfilePipe = new DTOProfileInfoPipe(args)
    val friendInfoPipe = new FriendInfoPipe(args)
    val checkinGrouperPipe = new CheckinGrouperFunction(args)
    val checkinInfoPipe = new CheckinInfoPipe(args)
    val apiCalls = new APICalls(args)
    val metaphoner = new StemAndMetaphoneEmployer()
    val levenshteiner = new Levenshtein()
    val checker = new EmployerCheckinMatch
    val haversiner = new Haversine
    val coworkerPipe = new CoworkerFinderFunction((args))
    val friendGrouper = new FriendGrouperFunction(args)
    val dtoPlacesInfoPipe = new DTOPlacesInfoPipe(args)
    val gendperpipe = new GenderInfoReadPipe(args)
    val joinedProfiles = dtoProfileGetPipe.getDTOProfileInfoInTuples(data)
    val certainityScore = new CertainityScorePipe(args)
    val jobTypeToRun = new JobTypeToRun(args)
    val internalAnalysisJob = new InternalAnalysisJob(args)

    val filteredProfiles = joinedProfiles.project(('key, 'uname, 'fbid, 'lnid, 'worked, 'city, 'worktitle))
            .map('worked ->('stemmedWorked, 'mtphnWorked)) {
            fields: String =>
            val (worked) = fields
            val stemmedWorked = StemAndMetaphoneEmployer.getStemmed(worked)
            val mtphnWorked = StemAndMetaphoneEmployer.getStemmedMetaphone(worked)

            (stemmedWorked, mtphnWorked)
    }.project(('key, 'uname, 'fbid, 'lnid, 'stemmedWorked, 'mtphnWorked, 'city, 'worktitle))

    val placesPipe = dtoPlacesInfoPipe.getPlacesInfo(TextLine(placesData).read)
            .project(('geometryType, 'geometryLatitude, 'geometryLongitude, 'type, 'id, 'propertiesProvince, 'propertiesCity, 'propertiesName, 'propertiesTags, 'propertiesCountry,
            'classifiersCategory, 'classifiersType, 'classifiersSubcategory, 'propertiesPhone, 'propertiesHref, 'propertiesAddress, 'propertiesOwner, 'propertiesPostcode))
            .map('propertiesName ->('stemmedName, 'mtphnName)) {
        fields: String =>
            val (placeName) = fields
            val stemmedName = StemAndMetaphoneEmployer.getStemmed(placeName)
            val mtphnName = StemAndMetaphoneEmployer.getStemmedMetaphone(placeName)
            (stemmedName, mtphnName)
    }.project(('geometryType, 'geometryLatitude, 'geometryLongitude, 'type, 'id, 'propertiesProvince, 'propertiesCity, 'stemmedName, 'mtphnName, 'propertiesTags, 'propertiesCountry,
            'classifiersCategory, 'classifiersType, 'classifiersSubcategory, 'propertiesPhone, 'propertiesHref, 'propertiesAddress, 'propertiesOwner, 'propertiesPostcode))


    var friends = friendInfoPipe.friendsDataPipe(TextLine(finp).read)

    val friendData = TextLine(finp).read.project('line)

    val chkindata = checkinGrouperPipe.groupCheckins(TextLine(chkininputData).read)

    val profilesAndCheckins = filteredProfiles.joinWithLarger('key -> 'keyid, chkindata).project(('key, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'loc))

    val employerGroupedServiceProfiles = employerGroupedServiceProfilePipe.getDTOProfileInfoInTuples(data).project(('key, 'worked))

    val serviceIds = joinedProfiles.project(('key, 'fbid, 'lnid)).rename(('key, 'fbid, 'lnid) ->('row_keyfrnd, 'fbId, 'lnId))

    val friendsForCoworker = friendGrouper.groupFriends(friendData)

    val numberOfFriends = friendsForCoworker.groupBy('userProfileId) {
        _.size
    }.rename('size -> 'numberOfFriends).project(('userProfileId, 'numberOfFriends))

    val coworkerCheckins = coworkerPipe.findCoworkerCheckinsPipe(employerGroupedServiceProfiles, friendsForCoworker, serviceIds, chkindata)

    val findcityfromchkins = checkinInfoPipe.findClusteroidofUserFromChkins(profilesAndCheckins.++(coworkerCheckins))

    val filteredProfilesWithScore = certainityScore.stemmingAndScore(filteredProfiles, findcityfromchkins, placesPipe, numberOfFriends)

    val seqModel = SequenceFile(bayestrainingmodel, Fields.ALL).read.mapTo((0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10) ->('key, 'token, 'featureCount, 'termDocCount, 'docCount, 'logTF, 'logIDF, 'logTFIDF, 'normTFIDF, 'rms, 'sigmak)) {
        fields: (String, String, Int, Int, Int, Double, Double, Double, Double, Double, Double) => fields

    }

    val jobRunPipeResults = jobTypeToRun.jobTypeToRun(jobtypeTorun, filteredProfilesWithScore, seqModel, trainedseqmodel)
    val groupByServiceType = internalAnalysisJob.internalAnalysisGroupByServiceType(data)
    val uniqueProfiles = internalAnalysisJob.internalAnalysisUniqueProfiles(data)
    val groupByCity = internalAnalysisJob.internalAnalysisGroupByCity(joinedProfiles)
    val (returnpipecity, returnpipecountry, returnpipework) = internalAnalysisJob.internalAnalysisGroupByCityCountryWorktitle(filteredProfilesWithScore, placesPipe, jobRunPipeResults, geohashsectorsize) //'key, 'uname, 'fbid, 'lnid, 'city, 'worktitle, 'lat, 'long, 'stemmedWorked, 'certaintyScore, 'numberOfFriends

    val profilesWithScore =  joinedProfiles.rename('key->'rowkey).project('rowkey,'edegree, 'eyear)
            .joinWithLarger('rowkey->'key,filteredProfilesWithScore).project('rowkey, 'uname, 'fbid, 'lnid,'edegree, 'eyear, 'city, 'worktitle, 'lat, 'long, 'stemmedWorked, 'certaintyScore, 'numberOfFriends)


    returnpipework.write(TextLine(groupworktitle))
    returnpipecountry.write(TextLine(groupcountry))
    returnpipecity.write(TextLine(groupcity))
    jobRunPipeResults.write(TextLine(jobOutputclasslabel))
    groupByServiceType.write(TextLine(serviceCount))
    uniqueProfiles.write(TextLine(profileCount))
    groupByCity.write(TextLine(geoCount))
    profilesWithScore.write(TextLine(jobOutput))

}


object DataAnalyser {

}*/

package com.sonar.expedition.scrawler.jobs

import com.sonar.expedition.scrawler.apis.APICalls
import com.sonar.expedition.scrawler.util._
import com.twitter.scalding._
import com.sonar.expedition.scrawler.util.CommonFunctions._
import com.sonar.expedition.scrawler.pipes._
import scala.util.matching.Regex
import cascading.pipe.joiner._
import com.twitter.scalding.TextLine
import cascading.tuple.Fields
import com.lambdaworks.jacks._
import tools.nsc.io.Streamable.Bytes
import ch.hsr.geohash.GeoHash


/*

       com.sonar.expedition.scrawler.jobs.DataAnalyser --hdfs --serviceProfileData "/tmp/serviceProfileData.txt" --friendData "/tmp/friendData.txt"  --checkinData "/tmp/checkinDatatest.txt" --placesData "/tmp/places_dump_US.geojson.txt"
        --output "/tmp/dataAnalyseroutput.txt" --occupationCodetsv "/tmp/occupationCodetsv.txt" --male "/tmp/male.txt" --female "/tmp/female.txt" --occupationCodeTsvOutpipe "/tmp/occupationCodeTsvOutpipe"
        --genderdataOutpipe "/tmp/genderdataOutpipe" --bayestrainingmodel "/tmp/bayestrainingmodel" --outputclassify "/tmp/jobclassified" --genderoutput "/tmp/genderoutput"  --outputjobtypes "/tmp/outputjobtypes"  --jobtype "3"
         --profileCount "/tmp/profileCount.txt" --serviceCount "/tmp/serviceCount.txt" --geoCount "/tmp/geoCount.txt
         --debug1 "/tmp/debug1" --debug2 "/tmp/debug2"  --debug3 "/tmp/debug3" --debug4 "/tmp/debug4" --debug5 "/tmp/debug5" --debug6 "/tmp/debug6"  --debug7 "/tmp/debug7" --debug8 "/tmp/debug8" --debug8 "/tmp/debug9"
         --geohashsectorsize "20"


     --jobtype "3"
        1 for only no gender and job classification
        2 for only gender
        3 for only job classification
        4 for both

        com.sonar.expedition.scrawler.jobs.DataAnalyser --hdfs --serviceProfileData "/tmp/serviceProfileData.txt" --friendData "/tmp/friendData.txt" --checkinData "/tmp/checkinDatatest.txt" --placesData "/tmp/places_dump_US.geojson.txt" --output "/tmp/dataAnalyseroutput.txt" --occupationCodetsv "/tmp/occupationCodetsv.txt" --occupationCodeTsvOutpipe "/tmp/occupationCodeTsvOutpipe" --genderdataOutpipe "/tmp/genderdataOutpipe" --bayestrainingmodel "/tmp/bayestrainingmodel" --outputclassify "/tmp/jobclassified" --genderoutput "/tmp/genderoutput"  --outputjobtypes "/tmp/outputjobtypes"  --jobtype "3"  --profileCount "/tmp/profileCount.txt" --serviceCount "/tmp/serviceCount.txt" --geoCount "/tmp/geoCount.txt"

        com.sonar.expedition.scrawler.jobs.DataAnalyser --hdfs --serviceProfileData "/tmp/serviceProfileData.txt" --friendData "/tmp/friendData.txt" --checkinData "/tmp/checkinDatatest.txt" --placesData "/tmp/places_dump_US.geojson.txt" --output "/tmp/dataAnalyseroutput.txt" --occupationCodetsv "/tmp/occupationCodetsv.txt" --occupationCodeTsvOutpipe "/tmp/occupationCodeTsvOutpipe" --genderdataOutpipe "/tmp/genderdataOutpipe" --bayestrainingmodel "/tmp/bayestrainingmodel" --outputclassify "/tmp/jobclassified" --genderoutput "/tmp/genderoutput"  --outputjobtypes "/tmp/outputjobtypes"  --jobtype "4"  --profileCount "/tmp/profileCount.txt" --serviceCount "/tmp/serviceCount.txt" --geoCount "/tmp/geoCount.txt" --trainedseqmodel  "/tmp/trainedseqmodel" --debug1 "/tmp/debug1" --debug2 "/tmp/debug2"  --debug3 "/tmp/debug3" --debug4 "/tmp/debug4" --debug5 "/tmp/debug5" --debug6 "/tmp/debug6"  --debug7 "/tmp/debug7" --debug8 "/tmp/debug8" --debug9 "/tmp/debug9" --geoHash "/tmp/geoHash" --geohashsectorsize "1000" --groupcountry "/tmp/groupcountry" --groupworktitle "/tmp/groupworktitle"  --groupcity "/tmp/groupcity"
 */


class DataAnalyser(args: Args) extends Job(args) {

    val inputData = args("serviceProfileData")
    val finp = args("friendData")
    val chkininputData = args("checkinData")
    val jobOutput = args("output")
    val jobOutputclasslabel = args("outputclassify")
    val placesData = args("placesData")
    val bayestrainingmodel = args("bayestrainingmodel")
    val genderoutput = args("genderoutput")
    val profileCount = args("profileCount")
    val serviceCount = args("serviceCount")
    val geoCount = args("geoCount")
    val jobtypeTorun = args("jobtype")
    val trainedseqmodel = args("trainedseqmodel")
    val geohashsectorsize = args.getOrElse("geohashsectorsize", "20").toInt
    val groupworktitle = args("groupworktitle")
    val groupcountry = args("groupcountry")
    val groupcity = args("groupcity")


    val data = (TextLine(inputData).read.project('line).flatMap(('line) ->('id, 'serviceType, 'jsondata)) {
        line: String => {
            line match {
                case ServiceProfileExtractLine(userProfileId, serviceType, json) => List((userProfileId, serviceType, json))
                case _ => List.empty
            }
        }
    }).project(('id, 'serviceType, 'jsondata))


    val dtoProfileGetPipe = new DTOProfileInfoPipe(args)
    val employerGroupedServiceProfilePipe = new DTOProfileInfoPipe(args)
    val friendInfoPipe = new FriendInfoPipe(args)
    val checkinGrouperPipe = new CheckinGrouperFunction(args)
    val checkinInfoPipe = new CheckinInfoPipe(args)
    val apiCalls = new APICalls(args)
    val metaphoner = new StemAndMetaphoneEmployer()
    val levenshteiner = new Levenshtein()
    val checker = new EmployerCheckinMatch
    val haversiner = new Haversine
    val coworkerPipe = new CoworkerFinderFunction((args))
    val friendGrouper = new FriendGrouperFunction(args)
    val dtoPlacesInfoPipe = new DTOPlacesInfoPipe(args)
    val gendperpipe = new GenderInfoReadPipe(args)
    val joinedProfiles = dtoProfileGetPipe.getDTOProfileInfoInTuples(data)
    val certainityScore = new CertainityScorePipe(args)
    val jobTypeToRun = new JobTypeToRun(args)
    val internalAnalysisJob = new InternalAnalysisJob(args)


    val filteredProfiles = joinedProfiles.project(('key, 'uname, 'fbid, 'lnid, 'worked, 'city, 'worktitle))
            .map('worked ->('stemmedWorked, 'mtphnWorked)) {
        fields: String =>
            val (worked) = fields
            val stemmedWorked = StemAndMetaphoneEmployer.getStemmed(worked)
            val mtphnWorked = StemAndMetaphoneEmployer.getStemmedMetaphone(worked)

            (stemmedWorked, mtphnWorked)
    }.project(('key, 'uname, 'fbid, 'lnid, 'stemmedWorked, 'mtphnWorked, 'city, 'worktitle))

    val placesPipe = dtoPlacesInfoPipe.getPlacesInfo(TextLine(placesData).read)
            .project(('geometryType, 'geometryLatitude, 'geometryLongitude, 'type, 'id, 'propertiesProvince, 'propertiesCity, 'propertiesName, 'propertiesTags, 'propertiesCountry,
            'classifiersCategory, 'classifiersType, 'classifiersSubcategory, 'propertiesPhone, 'propertiesHref, 'propertiesAddress, 'propertiesOwner, 'propertiesPostcode))
            .map('propertiesName ->('stemmedName, 'mtphnName)) {
        fields: String =>
            val (placeName) = fields
            val stemmedName = StemAndMetaphoneEmployer.getStemmed(placeName)
            val mtphnName = StemAndMetaphoneEmployer.getStemmedMetaphone(placeName)
            (stemmedName, mtphnName)
    }.project(('geometryType, 'geometryLatitude, 'geometryLongitude, 'type, 'id, 'propertiesProvince, 'propertiesCity, 'stemmedName, 'mtphnName, 'propertiesTags, 'propertiesCountry,
            'classifiersCategory, 'classifiersType, 'classifiersSubcategory, 'propertiesPhone, 'propertiesHref, 'propertiesAddress, 'propertiesOwner, 'propertiesPostcode))


    val friendData = TextLine(finp).read.project('line)

    val chkindata = checkinGrouperPipe.groupCheckins(TextLine(chkininputData).read)

    val profilesAndCheckins = filteredProfiles.joinWithLarger('key -> 'keyid, chkindata).project(('key, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'loc))

    val employerGroupedServiceProfiles = employerGroupedServiceProfilePipe.getDTOProfileInfoInTuples(data).project(('key, 'worked))

    val serviceIds = joinedProfiles.project(('key, 'fbid, 'lnid)).rename(('key, 'fbid, 'lnid) ->('row_keyfrnd, 'fbId, 'lnId))

    val friendsForCoworker = friendGrouper.groupFriends(friendData)

    val numberOfFriends = friendsForCoworker.groupBy('userProfileId) {
        _.size
    }.rename('size -> 'numberOfFriends).project(('userProfileId, 'numberOfFriends))

    val coworkerCheckins = coworkerPipe.findCoworkerCheckinsPipe(employerGroupedServiceProfiles, friendsForCoworker, serviceIds, chkindata)

    val findcityfromchkins = checkinInfoPipe.findClusteroidofUserFromChkins(profilesAndCheckins.++(coworkerCheckins))


    val filteredProfilesWithScore = certainityScore.stemmingAndScore(filteredProfiles, findcityfromchkins, placesPipe, numberOfFriends)
            .write(TextLine(jobOutput))


    val seqModel = SequenceFile(bayestrainingmodel, Fields.ALL).read.mapTo((0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10) ->('key, 'token, 'featureCount, 'termDocCount, 'docCount, 'logTF, 'logIDF, 'logTFIDF, 'normTFIDF, 'rms, 'sigmak)) {
        fields: (String, String, Int, Int, Int, Double, Double, Double, Double, Double, Double) => fields

    }

    val jobRunPipeResults = jobTypeToRun.jobTypeToRun(jobtypeTorun, filteredProfilesWithScore, seqModel, trainedseqmodel)

    internalAnalysisJob.internalAnalysisGroupByServiceType(data).write(TextLine(serviceCount))
    internalAnalysisJob.internalAnalysisUniqueProfiles(data).write(TextLine(profileCount))
    internalAnalysisJob.internalAnalysisGroupByCity(joinedProfiles).write(TextLine(geoCount))

    val (returnpipecity, returnpipecountry, returnpipework) = internalAnalysisJob.internalAnalysisGroupByCityCountryWorktitle(filteredProfilesWithScore, placesPipe, jobRunPipeResults, geohashsectorsize) //'key, 'uname, 'fbid, 'lnid, 'city, 'worktitle, 'lat, 'long, 'stemmedWorked, 'certaintyScore, 'numberOfFriends
    returnpipework.write(TextLine(groupworktitle))
    returnpipecountry.write(TextLine(groupcountry))
    returnpipecity.write(TextLine(groupcity))

}


object DataAnalyser {

}
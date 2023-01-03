using MySql.Data.MySqlClient;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Web;
using unirest_net.http;

Main();

async void Main()
{
    DeleteFile("error.log");
    DeleteFile("output.log");
    DeleteFile("ReverseGeoJson.log");
    string conn = CreateRemoteSQLConnection("IP", "PORT", "UN", "PW", "DATABASE");
    int itemCount = await GetCountSQLAsync("SELECT count(date_time) FROM Dirty", conn);
    Console.WriteLine("Item Count: " + itemCount);
    int mainLoop = itemCount / 400;
    int smallLoop = itemCount - (mainLoop * 400);
    Siting[] arrDBItems = null;
    for (int i = 0; i < mainLoop; i++)//All main cleaning code lies in these two loops // Chnage I to Numberdone/400 to find closest lower done bound to continue running with minimal overlap.
    {
        arrDBItems = await GetStringArraySQLAsync("SELECT date_time, city, state, country, shape, duration, comments, date_posted, latitude, longitude FROM Dirty LIMIT " + i * 400 + ",400", conn, 400);
        for (int c = 0; c < 400; c++)//Loop throught the 400 rows
        {
            CheckEverythingAsync(c, i, conn, arrDBItems);
        }
    }
    arrDBItems = await GetStringArraySQLAsync("SELECT date_time, city, state, country, shape, duration, comments, date_posted, latitude, longitude FROM Dirty LIMIT " + (mainLoop * 400) + ",400", conn, smallLoop);
    for (int c = 0; c < smallLoop; c++)//Loop throught the 400 rows
    {
        CheckEverythingAsync(c, mainLoop, conn, arrDBItems);
    }
}

async void CheckEverythingAsync(int c, int i, string conn, Siting[] arrDBItems)
{
    string output = ((i * 400) + c + 1) + " :: " + DateTime.Now.ToString() + " ::OLD:: " + arrDBItems[c].date_time + "\t" + arrDBItems[c].city
            + "\t" + arrDBItems[c].state + "\t" + arrDBItems[c].country + "\t" + arrDBItems[c].shape + "\t"
            + arrDBItems[c].duration + "\t" + arrDBItems[c].comments + "\t" + arrDBItems[c].date_posted + "\t"
            + arrDBItems[c].latitude + "\t" + arrDBItems[c].longitude + "\n";
    Console.WriteLine(output);
    AppendToFile(output, "output.log");
    var chckLocaction = CheckForMissingLocationProperties(arrDBItems[c]);//Not sure about this
    arrDBItems[c] = chckLocaction.Item1;
    bool flag = chckLocaction.Item2;
    arrDBItems[c].shape = CheckShape(arrDBItems[c].shape);
    arrDBItems[c].comments = CheckComment(arrDBItems[c].comments);
    arrDBItems[c].duration = CheckDuration(arrDBItems[c].duration);
    output = ((i * 400) + c + 1) + " :: " + DateTime.Now.ToString() + " ::New:: " + arrDBItems[c].date_time + "\t" + arrDBItems[c].city
                        + "\t" + arrDBItems[c].state + "\t" + arrDBItems[c].country + "\t" + arrDBItems[c].shape + "\t"
                        + arrDBItems[c].duration + "\t" + arrDBItems[c].comments + "\t" + arrDBItems[c].date_posted + "\t"
                        + arrDBItems[c].latitude + "\t" + arrDBItems[c].longitude + "\n";
    Console.WriteLine(output);    
    DateTime temp;
    if (DateTime.TryParse(arrDBItems[c].date_time, out temp) && arrDBItems[c].date_time != null && arrDBItems[c].date_time.Contains('/'))
    {
        if (DateTime.TryParse(arrDBItems[c].date_posted, out temp) && arrDBItems[c].date_posted != null && arrDBItems[c].date_posted.Contains('/'))
        {
            if (flag)
            {
                if (await CheckIfAlreadyinDBAsync(arrDBItems[c], conn) < 1)
                {
                    ReturnToDB(arrDBItems[c], conn);
                    AppendToFile(output, "output.log");
                }
            }
        }
    }
}

string CheckShape(string? shape)
{
    if (shape != null)
    {
        shape = ToUpperFirstLetter(shape);
    }
    else
    {
        shape = "Unknown";
    }
    return shape;
}

string CheckDuration(string? duration)
{
    if (duration == null)
    {
        duration = "0";
    }
    return duration.ToString();
}

async Task<int> CheckIfAlreadyinDBAsync(Siting siting, string DatabaseConnection)
{
    int count = 0;
    try
    {
        await using MySqlConnection conn = new MySqlConnection(DatabaseConnection);
        await conn.OpenAsync();
        MySqlCommand sqlCommand = conn.CreateCommand();
        sqlCommand.CommandText = "SELECT COUNT(date_time) FROM semiclean WHERE date_time = @date_time AND city = @city"
            + " AND  state = @state AND country = @country AND shape = @shape AND duration = @duration AND comments = @comments "
            + " AND date_posted = @date_posted AND latitude = @latitude AND longitude = @longitude";
        sqlCommand.Parameters.AddWithValue("@date_time", DateTime.Parse(siting.date_time));
        sqlCommand.Parameters.AddWithValue("@city", siting.city);
        sqlCommand.Parameters.AddWithValue("@state", siting.state);
        sqlCommand.Parameters.AddWithValue("@country", siting.country);
        sqlCommand.Parameters.AddWithValue("@shape", siting.shape);
        sqlCommand.Parameters.AddWithValue("@duration", new TimeSpan(0, 0, int.Parse(siting.duration)));
        sqlCommand.Parameters.AddWithValue("@comments", siting.comments);
        sqlCommand.Parameters.AddWithValue("@date_posted", DateTime.Parse(siting.date_posted).ToShortDateString());
        sqlCommand.Parameters.AddWithValue("@latitude", double.Parse(siting.latitude));
        sqlCommand.Parameters.AddWithValue("@longitude", double.Parse(siting.longitude));
        var temp = await sqlCommand.ExecuteScalarAsync();
        count = Convert.ToInt32(temp);
    }
    catch (MySqlException ex)
    {
        string errorMessages = "Index #" + "1" + "\n" +
                "Message: " + ex.Message + "\n" +
                "Stack Trace: " + ex.StackTrace + "\n" +
                "Source: " + ex.Source + "\n" +
                "Target Site: " + ex.TargetSite + "\n";
        Console.WriteLine(errorMessages);
        AppendToFile(errorMessages, "error.log");
    }
    return count;
}

async void ReturnToDB(Siting siting, string DatabaseConnection)
{
    try
    {
        await using MySqlConnection conn = new MySqlConnection(DatabaseConnection);
        await conn.OpenAsync();
        MySqlCommand sqlCommand = conn.CreateCommand();
        sqlCommand.CommandText = "INSERT INTO semiclean(date_time,city,state,country,shape,duration,comments,date_posted,"
            + "latitude,longitude) VALUES(@date_time,@city,@state,@country,@shape,@duration,@comments,@date_posted,"
            + "@latitude,@longitude)";//Comment removed for now
        sqlCommand.Parameters.AddWithValue("@date_time", DateTime.Parse(siting.date_time));
        sqlCommand.Parameters.AddWithValue("@city", siting.city);
        sqlCommand.Parameters.AddWithValue("@state", siting.state);
        sqlCommand.Parameters.AddWithValue("@country", siting.country);
        sqlCommand.Parameters.AddWithValue("@shape", siting.shape);
        sqlCommand.Parameters.AddWithValue("@duration", new TimeSpan(0, 0, int.Parse(siting.duration)));
        sqlCommand.Parameters.AddWithValue("@comments", siting.comments);
        sqlCommand.Parameters.AddWithValue("@date_posted", DateTime.Parse(siting.date_posted).ToShortDateString());
        sqlCommand.Parameters.AddWithValue("@latitude", double.Parse(siting.latitude));
        sqlCommand.Parameters.AddWithValue("@longitude", double.Parse(siting.longitude));
        await sqlCommand.ExecuteNonQueryAsync();
    }
    catch (MySqlException ex)
    {
        string errorMessages = "Index #" + "1" + "\n" +
                "Message: " + ex.Message + "\n" +
                "Stack Trace: " + ex.StackTrace + "\n" +
                "Source: " + ex.Source + "\n" +
                "Target Site: " + ex.TargetSite + "\n";
        Console.WriteLine(errorMessages);
        AppendToFile(errorMessages, "error.log");
    }
}

string CheckComment(string? input)
{
    if (input != null)
    {
        int iIndex = input.IndexOf("&#");
        if (iIndex == -1)
        {
            return input;
        }
        else
        {
            while (iIndex != -1)
            {
                string temp = input.Substring(iIndex, 4);
                string temp2 = HttpUtility.HtmlDecode(temp + ";");
                input = input.Replace(temp, temp2);
                iIndex = input.IndexOf("&#");
            }
            return input;
        }
    }
    else
    {
        return "No Comment";
    }

}

(Siting, bool) CheckForMissingLocationProperties(Siting arrRow)//Checks for if there is a missing value in city, state or country.
{
    bool flag = true;
    string[] arrTemp;
    if (arrRow.latitude == "0" || arrRow.latitude == null || arrRow.latitude == "" || arrRow.longitude == "0" || arrRow.longitude == null || arrRow.longitude == "")
    {
        if (arrRow.city != null && arrRow.city != "")//Checks if city name contains any dodgy stuff
        {
            if (arrRow.state != null && arrRow.state != "" && !arrRow.state.Any(ch => !Char.IsLetter(ch)))//Checks if city name contains any dodgy stuff
            {
                if (arrRow.city.Contains('('))
                {
                    string city = arrRow.city.Substring(0, arrRow.city.IndexOf('('));
                    if (city.Length == 0)
                    {
                        string error = arrRow.date_time + "\t" + arrRow.city + "\t" + arrRow.state + "\t" + arrRow.country + "\t" + arrRow.shape + "\t" + arrRow.duration + "\t" + arrRow.comments + "\t" + arrRow.date_posted + "\t" + arrRow.latitude + "\t" + arrRow.longitude + "\n";
                        AppendToFile(error, "error.log");
                        flag = false;
                    }
                    else
                    {
                        if (PullGeoJson(city, arrRow.country, out arrTemp))
                        {
                            arrRow.latitude = arrTemp[0];
                            arrRow.longitude = arrTemp[1];
                        }
                        else
                        {
                            string error = arrRow.date_time + "\t" + arrRow.city + "\t" + arrRow.state + "\t" + arrRow.country + "\t" + arrRow.shape + "\t" + arrRow.duration + "\t" + arrRow.comments + "\t" + arrRow.date_posted + "\t" + arrRow.latitude + "\t" + arrRow.longitude + "\n";
                            AppendToFile(error, "error.log");
                            flag = false;
                        }
                    }
                }
                else
                {
                    if (PullGeoJson(arrRow.city, arrRow.state, out arrTemp))
                    {
                        arrRow.latitude = arrTemp[0];
                        arrRow.longitude = arrTemp[1];
                    }
                    else
                    {
                        string error = arrRow.date_time + "\t" + arrRow.city + "\t" + arrRow.state + "\t" + arrRow.country + "\t" + arrRow.shape + "\t" + arrRow.duration + "\t" + arrRow.comments + "\t" + arrRow.date_posted + "\t" + arrRow.latitude + "\t" + arrRow.longitude + "\n";
                        AppendToFile(error, "error.log");
                        flag = false;
                    }
                }

            }
            else
            {
                if (arrRow.city.Contains('('))
                {
                    string city = arrRow.city.Substring(0, arrRow.city.IndexOf('('));
                    if (city.Length == 0)
                    {
                        string error = arrRow.date_time + "\t" + arrRow.city + "\t" + arrRow.state + "\t" + arrRow.country + "\t" + arrRow.shape + "\t" + arrRow.duration + "\t" + arrRow.comments + "\t" + arrRow.date_posted + "\t" + arrRow.latitude + "\t" + arrRow.longitude + "\n";
                        AppendToFile(error, "error.log");
                    }
                    else
                    {
                        if (PullGeoJson(city, arrRow.country, out arrTemp))
                        {
                            arrRow.latitude = arrTemp[0];
                            arrRow.longitude = arrTemp[1];
                        }
                        else
                        {
                            string error = arrRow.date_time + "\t" + arrRow.city + "\t" + arrRow.state + "\t" + arrRow.country + "\t" + arrRow.shape + "\t" + arrRow.duration + "\t" + arrRow.comments + "\t" + arrRow.date_posted + "\t" + arrRow.latitude + "\t" + arrRow.longitude + "\n";
                            AppendToFile(error, "error.log");
                            flag = false;
                        }
                    }

                }
                else
                {
                    if (PullGeoJson(arrRow.city, arrRow.country, out arrTemp))
                    {
                        arrRow.latitude = arrTemp[0];
                        arrRow.longitude = arrTemp[1];
                    }
                    else
                    {
                        string error = arrRow.date_time + "\t" + arrRow.city + "\t" + arrRow.state + "\t" + arrRow.country + "\t" + arrRow.shape + "\t" + arrRow.duration + "\t" + arrRow.comments + "\t" + arrRow.date_posted + "\t" + arrRow.latitude + "\t" + arrRow.longitude + "\n";
                        AppendToFile(error, "error.log");
                        flag = false;
                    }
                }
            }

        }
    }
    if (arrRow.city == null || arrRow.city == "" || arrRow.city.Any(ch => !Char.IsLetter(ch)) || arrRow.city == arrRow.city.ToLower()
        || arrRow.state == null || arrRow.state == "" || arrRow.state.Any(ch => !Char.IsLetter(ch)) || arrRow.state == arrRow.state.ToLower()
        || arrRow.country == null || arrRow.country == "" || arrRow.country.Any(ch => !Char.IsLetter(ch)) || arrRow.country == arrRow.country.ToLower())//Checks if city name contains any dodgy stuff
    {
        if (!PullReverseGeoJson(arrRow.latitude, arrRow.longitude, out arrTemp))
        {
            string error = arrRow.date_time + "\t" + arrRow.city + "\t" + arrRow.state + "\t" + arrRow.country + "\t" + arrRow.shape + "\t" + arrRow.duration + "\t" + arrRow.comments + "\t" + arrRow.date_posted + "\t" + arrRow.latitude + "\t" + arrRow.longitude + "\n";
            AppendToFile(error, "error.log");
            flag = false;
        }
        else
        {
            arrRow.city = arrTemp[0];
            arrRow.state = arrTemp[1];
            arrRow.country = arrTemp[2];
            if (arrRow.city == "Rural")
            {
                string error = arrRow.date_time + "\t" + arrRow.city + "\t" + arrRow.state + "\t" + arrRow.country + "\t" + arrRow.shape + "\t" + arrRow.duration + "\t" + arrRow.comments + "\t" + arrRow.date_posted + "\t" + arrRow.latitude + "\t" + arrRow.longitude + "\n";
                AppendToFile(error, "error.log");
            }
        }
    }
    return (arrRow,flag);
}

bool PullReverseGeoJson(string lat, string lon, out string[] output)//Take the lat and lon and returns the value of the missing item if possible
{
    HttpResponse<string> response = UnirestReverseLocationRequest(lat, lon);
    JObject json = JObject.Parse(response.Body.ToString());
    output = new string[3];
    string log = "";
    if (json.GetValue("address") != null)
    {
        foreach (var item in json.GetValue("address"))
        {
            log += item.ToString() + ";";
        }
        log += lat + ";" + lon + ";\n";
        AppendToFile(log, "ReverseGeoJson.log");
        if (!(json.GetValue("address").Value<string>("city") == null))
        {
            output[0] = json.GetValue("address").Value<string>("city");
        }
        else if (!(json.GetValue("address").Value<string>("town") == null))
        {
            output[0] = json.GetValue("address").Value<string>("town");
        }
        else if (!(json.GetValue("address").Value<string>("village") == null))
        {
            output[0] = json.GetValue("address").Value<string>("village");
        }
        else if (!(json.GetValue("address").Value<string>("suburb") == null))
        {
            output[0] = json.GetValue("address").Value<string>("suburb");
        }
        else
        {
            output[0] = "Rural";
        }
        //Not all lat long have a city or town
        if (!(json.GetValue("address").Value<string>("state") == null))
        {
            output[1] = json.GetValue("address").Value<string>("state");
        }
        else if (!(json.GetValue("address").Value<string>("region") == null))
        {
            output[1] = json.GetValue("address").Value<string>("region");
        }
        else
        {
            output[1] = "No State";
        }        
        output[2] = json.GetValue("address").Value<string>("country");
    }
    else
    {
        output[0] = "Unknown";
        output[1] = "Unknown";
        output[2] = "Unknown";
    }
    return !(output[0] == "Unknown" && output[1] == "Unknown" && output[2] == "Unknown");
}

bool PullGeoJson(string city, string state, out string[] output)//Take the lat and lon and returns the value of the missing item if possible
{
    HttpResponse<string> response = UnirestLocationRequest(city, state);
    var data = JsonConvert.DeserializeObject<List<GeoModel>>(response.Body.ToString());
    output = new string[2];
    AppendToFile("GeoJson List Length:  " + data.Count().ToString(),"output.log");
    if (data.Count() > 0 && data[0].lat != null && data[0].lon != null)
    {
        AppendToFile("City:  " + city + "  State/Country:  " + state + "  Latitude:  " + data[0].lat + "  Longitude:  " + data[0].lon + "\n","output.log");
        output[0] = data[0].lat;
        output[1] = data[0].lon;
        //TO DO: Implement TryParse - https://learn.microsoft.com/en-us/dotnet/csharp/language-reference/operators/lambda-operator
    }
    else
    {
        output[0] = "0";
        output[1] = "0";
        Console.WriteLine("GeoJson ERROR \n");
    }
    return !(output[0] == "0" && output[1] == "0");
}

async Task<int> GetCountSQLAsync(string Command, string DatabaseConnection)//Returns row count from and sql statement
{
    int count = 0;
    try
    {
        await using MySqlConnection conn = new MySqlConnection(DatabaseConnection);
        await conn.OpenAsync();
        using MySqlCommand sqlCommand = new MySqlCommand(Command, conn);
        var temp = await sqlCommand.ExecuteScalarAsync();
        count = Convert.ToInt32(temp);
    }
    catch (MySqlException ex)
    {
        string errorMessages = "Index #" + "1" + "\n" +
                "Message: " + ex.Message + "\n" +
                "Stack Trace: " + ex.StackTrace + "\n" +
                "Source: " + ex.Source + "\n" +
                "Target Site: " + ex.TargetSite + "\n";
        Console.WriteLine(errorMessages);
        AppendToFile(errorMessages, "error.log");
    }
    return count;

}

string CreateRemoteSQLConnection(string ServerAddress, string Port, string User, string Password, string DatabaseName)//Method to create dynamic external sql connection string
{
    return "server=" + ServerAddress + ";port=" + Port + ";user id=" + User + ";Password=" + Password + ";database="
        + DatabaseName + "; pooling = false; convert zero datetime=True";
}

void AppendToFile(string LineToAppend, string FileName)//Textfile method
{
    File.AppendAllText(Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location) + "\\Logs\\" + FileName, LineToAppend);
}

void DeleteFile(string FileName)//Textfile method
{
    if (File.Exists(Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location) + "\\Logs\\" + FileName))
    {
        File.Delete(Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location) + "\\Logs\\" + FileName);
    }
}

async Task<Siting[]> GetStringArraySQLAsync(string Command, string DatabaseConnection, int AmountOfRows)//Returns an array of Sitings
{
    Siting[] output = new Siting[AmountOfRows];//latitude,longitude
    try
    {
        await using MySqlConnection conn = new MySqlConnection(DatabaseConnection);
        int i = 0;
        await conn.OpenAsync();
        using MySqlCommand sqlCommand = new MySqlCommand(Command, conn);
        await using var dataReader = await sqlCommand.ExecuteReaderAsync();
        while (await dataReader.ReadAsync())
        {
            output[i] = new Siting();
            output[i].date_time = dataReader.GetValue(0).ToString();
            output[i].city = dataReader.GetValue(1).ToString();
            output[i].state = dataReader.GetValue(2).ToString();
            output[i].country = dataReader.GetValue(3).ToString();
            output[i].shape = dataReader.GetValue(4).ToString();
            output[i].duration = dataReader.GetValue(5).ToString();
            output[i].comments = dataReader.GetValue(6).ToString();
            output[i].date_posted = dataReader.GetValue(7).ToString();
            output[i].latitude = dataReader.GetValue(8).ToString();
            output[i].longitude = dataReader.GetValue(9).ToString();
            i++;
        }
    }
    catch (MySqlException ex)
    {
        string errorMessages = "Index #" + "1" + "\n" +
                "Message: " + ex.Message + "\n" +
                "Stack Trace: " + ex.StackTrace + "\n" +
                "Source: " + ex.Source + "\n" +
                "Target Site: " + ex.TargetSite + "\n";
        Console.WriteLine(errorMessages);
        AppendToFile(errorMessages, "error.log");
    }
    return output;
}

async void NonQuerySQLAsync(string Command, string DatabaseConnection)//Used for executing a non query sql statement
{
    try
    {
        await using MySqlConnection conn = new MySqlConnection(DatabaseConnection);
        await conn.OpenAsync();
        using MySqlCommand sqlCommand = new MySqlCommand(Command, conn);
        await sqlCommand.ExecuteNonQueryAsync();
    }
    catch (MySqlException ex)
    {
        string errorMessages = "Index #" + "1" + "\n" +
                "Message: " + ex.Message + "\n" +
                "Stack Trace: " + ex.StackTrace + "\n" +
                "Source: " + ex.Source + "\n" +
                "Target Site: " + ex.TargetSite + "\n";
        Console.WriteLine(errorMessages);
        AppendToFile(errorMessages, "error.log");
    }
}

HttpResponse<string> UnirestReverseLocationRequest(string latitude, string longitude)
{
    HttpResponse<string> response = null;
    try
    {
        response = Unirest.get("https://geocode.maps.co/reverse?lat=" + latitude + "&lon=" + longitude + "").asJson<string>();
        //Thread.Sleep(200);
    }
    catch (HttpRequestException ex)
    {
        string errorMessages = "Index #" + "1" + "\n" +
                "Message: " + ex.Message + "\n" +
                "Stack Trace: " + ex.StackTrace + "\n" +
                "Source: " + ex.Source + "\n" +
                "Target Site: " + ex.TargetSite + "\n";
        Console.WriteLine(errorMessages);
        AppendToFile(errorMessages, "error.log");
    }
    return response;
}

HttpResponse<string> UnirestLocationRequest(string city, string state)
{
    HttpResponse<string> response = null;
    try
    {
        response = Unirest.get("https://geocode.maps.co/search?q=" + city + "," + state).asJson<string>();
        //Thread.Sleep(200);
    }
    catch (HttpRequestException ex)
    {
        string errorMessages = "Index #" + "1" + "\n" +
                "Message: " + ex.Message + "\n" +
                "Stack Trace: " + ex.StackTrace + "\n" +
                "Source: " + ex.Source + "\n" +
                "Target Site: " + ex.TargetSite + "\n";
        Console.WriteLine(errorMessages);
        AppendToFile(errorMessages, "error.log");
    }
    return response;
}

string ToUpperFirstLetter(string source)
{
    if (string.IsNullOrEmpty(source))
        return string.Empty;
    // convert to char array of the string
    char[] letters = source.ToCharArray();
    // upper case the first char
    letters[0] = char.ToUpper(letters[0]);
    // return the array made of the new char array
    return new string(letters);
}

class GeoModel
{
    [JsonProperty("place_id")]
    public long PlaceId { get; set; }

    [JsonProperty("lat")]
    public string? lat { get; set; }

    [JsonProperty("lon")]
    public string? lon { get; set; }

}

class Siting
{
    public string? date_time { get; set; }
    public string? city { get; set; }
    public string? state { get; set; }
    public string? country { get; set; }
    public string? shape { get; set; }
    public string? duration { get; set; }
    public string? comments { get; set; }
    public string? date_posted { get; set; }
    public string? latitude { get; set; }
    public string? longitude { get; set; }
}
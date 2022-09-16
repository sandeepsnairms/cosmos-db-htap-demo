using CallLog;

string subscriber = string.Empty;
string planId = string.Empty;
int offset = 90;
int count = 15;
foreach (string arg in Environment.GetCommandLineArgs())
{
    string[] keyval=arg.Split('=');
    switch (keyval[0])
    {
        case "offset":
            offset = int.Parse(keyval[1]);
            break;
        case "caller":
            subscriber=keyval[1];
            break;
        case "count":
            count = int.Parse(keyval[1]);
            break;

    }
}

Console.WriteLine($"Generating {count} calls with Offset of {offset} days");

CallLogger caller = new CallLogger();
for (int i = 1; i <= count; i++)
{   //generate fake calls
    caller.GenerateFakeCall(offset, subscriber);
    Console.WriteLine($"Generating Fake Call {i}");
}
Console.WriteLine($"Insert Started");

Task.Run(async () => { await caller.InsertCalls(); }).GetAwaiter().GetResult();

Console.WriteLine("Insert Complete");
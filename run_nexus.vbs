Set WshShell = CreateObject("WScript.Shell")
WshShell.Run "python C:\QuantCode\NexusCore\quantcode_nexus.py", 0, False
Set WshShell = Nothing
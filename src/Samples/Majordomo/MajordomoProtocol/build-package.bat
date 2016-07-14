"%~dp0"nuget Pack "%~dp0%2" -Prop Configuration=%1 -IncludeReferencedProjects -Symbols
"%~dp0"nuget push -source "D:\CSide\LocalNuget" %~dp0*.symbols.nupkg
# Creates summary CSV file based on indices created by Create-NgramFilesIndices script

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$scriptRoot = if ($PSScriptRoot) { $PSScriptRoot } else { (Get-Location).Path }
$indexDirectory = (Resolve-Path "$scriptRoot\..\Index").Path

$languages = @(
    'eng'
    'eng-us'
    'eng-gb'
    'eng-fiction'
    'chi_sim' # note '_' instead of '-'
    'fre'
    'ger'
    'heb'
    'ita'
    'rus'
    'spa'
)

$result = @()
foreach ($lang in $languages)
{
    $totalRow = [PSCustomObject]@{
        Language = $lang
    }

    $fileCountSum = 0
    $fileLengthSum = 0
    for ($i = 1; $i -le 5; $i++)
    {
        $csvFile = "$indexDirectory\$lang-$i.csv"

        Write-Host "Processing $csvFile"
        $csvData = Import-Csv -Encoding UTF8 $csvFile
        $summary = $csvData | Measure-Object Length -Sum

        $totalRow | Add-Member -NotePropertyName "File Count $i" -NotePropertyValue $summary.Count
        $totalRow | Add-Member -NotePropertyName "Length $i Bytes" -NotePropertyValue $summary.Sum
        $totalRow | Add-Member -NotePropertyName "Length $i GB" -NotePropertyValue ([math]::Round($summary.Sum / 1GB, 2))

        $fileCountSum += $summary.Count
        $fileLengthSum += $summary.Sum
    }

    $totalRow | Add-Member -NotePropertyName "File Count Sum" -NotePropertyValue $fileCountSum
    $totalRow | Add-Member -NotePropertyName "Length Sum Bytes" -NotePropertyValue $fileLengthSum
    $totalRow | Add-Member -NotePropertyName "Length Sum GB" -NotePropertyValue ([math]::Round($fileLengthSum / 1GB, 2))

    $result += $totalRow
}

$summary = $result | Measure-Object -Sum -Property @(
    'File Count 1'; 'File Count 2'; 'File Count 3'; 'File Count 4'; 'File Count 5'; 'File Count Sum';
    'Length 1 Bytes'; 'Length 2 Bytes'; 'Length 3 Bytes'; 'Length 4 Bytes'; 'Length 5 Bytes'; 'Length Sum Bytes'
)

$result += [PSCustomObject]@{
        Language = 'Total'
        'File Count 1'     = ($summary | ? Property -eq 'File Count 1').Sum
        'Length 1 Bytes'   = ($summary | ? Property -eq 'Length 1 Bytes').Sum
        'Length 1 GB'      = [math]::Round(($summary | ? Property -eq 'Length 1 Bytes').Sum / 1GB, 2)
        'File Count2'      = ($summary | ? Property -eq 'File Count 2').Sum
        'Length 2 Bytes'   = ($summary | ? Property -eq 'Length 2 Bytes').Sum
        'Length 2 GB'      = [math]::Round(($summary | ? Property -eq 'Length 2 Bytes').Sum / 1GB, 2)
        'File Count 3'     = ($summary | ? Property -eq 'File Count 3').Sum
        'Length 3 Bytes'   = ($summary | ? Property -eq 'Length 3 Bytes').Sum
        'Length 3 GB'      = [math]::Round(($summary | ? Property -eq 'Length 3 Bytes').Sum / 1GB, 2)
        'File Count 4'     = ($summary | ? Property -eq 'File Count 4').Sum
        'Length 4 Bytes'   = ($summary | ? Property -eq 'Length 4 Bytes').Sum
        'Length 4 GB'      = [math]::Round(($summary | ? Property -eq 'Length 4 Bytes').Sum / 1GB, 2)
        'File Count 5'     = ($summary | ? Property -eq 'File Count 5').Sum
        'Length 5 Bytes'   = ($summary | ? Property -eq 'Length 5 Bytes').Sum
        'Length 5 GB'      = [math]::Round(($summary | ? Property -eq 'Length 5 Bytes').Sum / 1GB, 2)
        'File Count Sum'   = ($summary | ? Property -eq 'File Count Sum').Sum
        'Length Sum Bytes' = ($summary | ? Property -eq 'Length Sum Bytes').Sum
        'Length Sum GB'    = [math]::Round(($summary | ? Property -eq 'Length Sum Bytes').Sum / 1GB, 2)
    }

# bytes lenghts are excluded as GB are easier to read
$result | 
    Select-Object -Property * -ExcludeProperty 'Length 1 Bytes', 'Length 2 Bytes', 'Length 3 Bytes', 'Length 4 Bytes', 'Length 5 Bytes', 'Length Sum Bytes' |
    ConvertTo-Csv -NoTypeInformation | Out-File "$indexDirectory\_summary_.csv" -Encoding utf8

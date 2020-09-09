# Creates CSV indices of ngram files with their sizes and MD5 hashes

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$scriptRoot = if ($PSScriptRoot) { $PSScriptRoot } else { (Get-Location).Path }
$outputDirectory = (Resolve-Path "$scriptRoot\..\Index").Path

$baseUrl = 'http://storage.googleapis.com/books/ngrams/books/20200217'

<#
  Numbers based on subpages of http://storage.googleapis.com/books/ngrams/books/datasetsv3.html
  Language => ( Ngram Word Count => Number of Files to Download )
#>
$lang2info = @{      
    'eng' = @{ 1 = 24; 2 = 589; 3 = 6881; 4 = 6668; 5 = 19423; } # English
    'eng-us' = @{ 1 = 14; 2 = 365; 3 = 4178; 4 = 3936; 5 = 11145; } # American English
    'eng-gb' = @{ 1 = 4; 2 = 118; 3 = 1283; 4 = 1154; 5 = 3098; } # British English
    'eng-fiction' = @{ 1 = 1; 2 = 47; 3 = 549; 4 = 515; 5 = 1449; } # English Fiction
    'chi_sim' = @{ 1 = 1; 2 = 6; 3 = 59; 4 = 46; 5 = 105; } # Chinese (simplified)
    'fre'= @{ 1 = 6; 2 = 115; 3 = 1197; 4 = 1104; 5 = 3071; } # French
    'ger'= @{ 1 = 8; 2 = 181; 3 = 1369; 4 = 1003; 5 = 2262; } # German
    'heb'= @{ 1 = 1; 2 = 10; 3 = 45; 4 = 25; 5 = 42; } # Hebrew
    'ita'= @{ 1 = 2; 2 = 60; 3 = 553; 4 = 427; 5 = 984; } # Italian
    'rus'= @{ 1 = 2; 2 = 69; 3 = 471; 4 = 313; 5 = 633; } # Russian
    'spa'= @{ 1 = 3; 2 = 73; 3 = 688; 4 = 571; 5 = 1415; } # Spanish
}

<#
    Example URL: http://storage.googleapis.com/books/ngrams/books/20200217/eng-fiction/1-00000-of-00001.gz
    Template: .../20200217/{language}/{ngram-word-count}-{0-indexed-file-no}-of-{all-files-count}.gz
#>
foreach ($lang in $lang2info.Keys)
{
    $info = $lang2info[$lang]
    foreach ($ngramIdx in $info.Keys)
    {
        $ngramFileInfo = @()
        $outFile = "$outputDirectory\$lang-$ngramIdx.csv"
        if (Test-Path $outFile) { continue }

        $fileCount = $info[$ngramIdx]
        for ($i = 0; $i -lt $fileCount; $i++) 
        {
            $filename = '{0}-{1:00000}-of-{2:00000}.gz' -f $ngramIdx, $i, $fileCount
            $url = "$baseUrl/$lang/$filename"
            $head = Invoke-WebRequest -Uri $url -UseBasicParsing -Method Head -Verbose
            $length = $head.Headers["x-goog-stored-content-length"]
            $hash = $head.Headers["x-goog-hash"]

            if ($hash -match '^crc32c=(.+),md5=(.+)$') {
                $md5base64 = $matches[2]
                $md5bytes = [System.Convert]::FromBase64String($md5base64)
                $md5hex = [BitConverter]::ToString($md5bytes).Replace("-", "").ToLower()
            } else {
                throw "MD5 hash missing"
            }

            $ngramFileInfo += [PSCustomObject]@{
                Url = $url
                Length = [Long]::Parse($length)
                MD5 = $md5hex
            }
        }

        Write-Host "Saving CSV index to $outFile"
        $ngramFileInfo | ConvertTo-Csv -NoTypeInformation | Out-File $outFile -Encoding UTF8
    }
}

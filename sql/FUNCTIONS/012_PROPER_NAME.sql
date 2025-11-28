-- Object Type: FUNCTIONS
CREATE OR REPLACE FUNCTION ALFA_EDW_DEV.PUBLIC.PROPER_NAME("S" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS '
var s = arguments[0];                     // <-- read arg this way in Snowflake JS UDFs
if (s === null || s === undefined) return null;

var t = String(s)
  .replace(/\\u2019/g, "''")               // normalize smart apostrophe to ''
  .toLowerCase()
  .trim()
  .replace(/\\s+/g, '' '');                 // collapse multiple spaces

function cap1(w){ return w ? w.charAt(0).toUpperCase() + w.slice(1) : w; }

function fixChunk(x){
  if (!x) return x;
  if (x.startsWith("mc")  && x.length >= 3) return "Mc"  + x.charAt(2).toUpperCase() + x.slice(3);
  if (x.startsWith("mac") && x.length >= 4) return "Mac" + x.charAt(3).toUpperCase() + x.slice(4);
  return cap1(x);
}

function fixHyphen(word){ return word.split("-").map(fixChunk).join("-"); }

return t
  .split(/\\s+/)                           // words
  .map(function(word){
    return word
      .split("''")                         // handle apostrophes
      .map(fixHyphen)                     // handle hyphens in each piece
      .map(function(part, i){ return i === 0 ? part : cap1(part); })
      .join("''");
  })
  .join(" ");
';
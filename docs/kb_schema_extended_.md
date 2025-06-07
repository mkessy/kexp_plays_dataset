### Knowledge Graph Schema for KEXP DJ Comments

This schema is designed to be comprehensive. You can select core components for initial implementation and iteratively add more detail.

#### Core Entities:

1.  **`Artist`**
    - **Description:** Represents a musical artist, which can be an individual or a group.
    - **Sub-types:** `Person`, `Group`
    - **Core Attributes:** `name` (String, Primary Name), `mb_id` (String, Optional), `artist_type` (Enum: PERSON, GROUP)
    - **Example:** "MIKE", "First Aid Kit", "Slowdive"
2.  **`WorkOfArt`** (Superclass for creative works)
    - **Core Attributes:** `title` (String)
3.  **`Song`** (Sub-type of `WorkOfArt`)
    - **Core Attributes:** (inherits `title`), `mb_recording_id` (String, Optional)
    - **Example:** "Pieces Of A Dream", "Lights and Music", "Hands"
4.  **`Album`** (Sub-type of `WorkOfArt`)
    - **Core Attributes:** (inherits `title`), `mb_release_group_id` (String, Optional), `album_type` (Enum: LP, EP, SINGLE, COMPILATION, etc.)
    - **Example:** "Showbiz", "In Ghost Colours", "Transformer"
5.  **`Release`** (A specific published instance of an Album or Song)
    - **Core Attributes:** `mb_release_id` (String, Optional), `release_date` (Date), `format` (String, e.g., "Vinyl", "CD", "Digital")
6.  **`Role`**

    - **Core Attributes:** `name` (String)
    - **Example:** "Producer", "Writer", "DJ", "Critic"

7.  **`Event`**

    - **Description:** Represents a happening, like a concert, festival, or broadcast.
    - **Core Attributes:** `event_name` (String, Optional, e.g., "SKOOKUM Festival"), `event_type` (Enum: CONCERT, FESTIVAL, BROADCAST, IN_STUDIO_SESSION, ALBUM_RELEASE_PARTY, TOUR), `event_date` (Date or DateRange)
    - **Example:** "Iceland Airwaves Music Festival", "KEXP session"

8.  **`Location`**
    - **Core Attributes:** `city` (String, Optional), `state_or_region` (String, Optional), `country` (String, Optional)
    - **Example:** "Seattle WA", "Vancouver, B.C.", "Cape Town, South Africa"
9.  **`Date`**
    - **Core Attributes:** `full_date` (ISO Date String), `year` (Integer), `month` (Integer, Optional), `day` (Integer, Optional), `qualifier` (String, e.g., "tonight", "this year", "recently")
10. **`Genre`**
    - **Core Attributes:** `name` (String, e.g., "Indie Rock", "Electronic")
11. **`RecordLabel`**
    - **Core Attributes:** `name` (String)
12. **`URL`**
    - **Core Attributes:** `address` (String), `link_type` (Enum: OFFICIAL_WEBSITE, BANDCAMP, NEWS_ARTICLE, MUSIC_VIDEO, SOCIAL_MEDIA, EVENT_PAGE, OTHER)
13. **`Venue`**

    - **Core Attributes:** `name` (String), `location` (Location)
    - **Example:** "The Showbox", "The Crocodile", "The Neptune"

14. **`Person`**
    - **Core Attributes:** `name` (String), `mb_id` (String, Optional)
    - **Example:** "PJ Harvey", "Ty Segall"

#### Core Relationships:

- **Artist-Work Relationships:**
  - `PERFORMED_BY` (Source: `WorkOfArt`, Target: `Artist`)
    - Example: `Song:"Lights and Music"` - `PERFORMED_BY` -> `Artist:"Cut Copy"`
  - `FEATURED_ARTIST_ON` (Source: `WorkOfArt`, Target: `Artist`)
    - Example: `Song:"Say Peace"` - `FEATURED_ARTIST_ON` -> `Artist:"PJ Morton"` (in comment for play_id: 2808398, assuming "Common feat. PJ Morton" implies PJ Morton is featured)
- **Artist-Artist Relationships:**
  - `HAS_MEMBER` (Source: `Group` (Artist), Target: `Person` (Artist))
    - Example: `Artist:"Florence and The Machine"` - `HAS_MEMBER` -> `Artist:"Florence Welch"`
  - `IS_PROJECT_OF` (Source: `Artist`, Target: `Artist`)
    - Example: "GØGGS" - `IS_PROJECT_OF` -> "Ty Segall" (derived from comment for play_id: 235605)
  - `KNOWN_AS` (Source: `Artist`, Target: `Artist`)
- **Work-Work Relationships:**
  - `APPEARS_ON` (Source: `Song`, Target: `Album`/`Release`)
  - `IS_COVER_OF` (Source: `Song`, Target: `Song`)
  - `SAMPLES_WORK` (Source: `Song`, Target: `Song`/`WorkOfArt`)
- **Work-Release Relationships:**
  - `RELEASED_AS` (Source: `Album`/`Song`, Target: `Release`)
  - `RELEASED_BY_LABEL` (Source: `Release`, Target: `RecordLabel`)
  - `RELEASED_ON_DATE` (Source: `Release`, Target: `Date`)
  - `RE_RELEASED_AS` (Source: `Release`, Target: `Release`)
- **Event-Related Relationships:**
  - `ARTIST_PERFORMED_AT_EVENT` (Source: `Artist`, Target: `Event`)
    - Example: `Artist:"PJ Harvey"` - `PERFORMED_AT_EVENT` -> `Event:"Iceland Airwaves Music Festival"`
  - `EVENT_OCCURRED_AT_LOCATION` (Source: `Event`, Target: `Location`)
    - Example: `Event:"SKOOKUM Festival"` - `EVENT_OCCURRED_AT_LOCATION` -> `Location:"Vancouver, B.C."`
  - `EVENT_OCCURRED_ON_DATE` (Source: `Event`, Target: `Date`)
- **General Descriptive Relationships:**
  - `HAS_GENRE` (Source: `Artist`/`WorkOfArt`, Target: `Genre`)
  - `ORIGINATES_FROM` (Source: `Artist`, Target: `Location`)
    - Example: `Artist:"Slowdive"` - `ORIGINATES_FROM` -> `Location:"England"` (implied, generally known)
  - `HAS_LINK` (Source: `Artist`/`WorkOfArt`/`Event`, Target: `URL`)
    - Example: `Artist:"Slowdive"` - `HAS_LINK` -> `URL:"http://blog.kexp.org/..."`

---

#### Extended Entities (for more detailed knowledge graph):

13. **`Influence`** (Can be an `Artist`, `Genre`, `WorkOfArt`, or descriptive text)
14. **`Role`** (e.g., "guitarist", "vocalist")
15. **`SoundDescription`** (Text)
16. **`LyricalTheme`** (Text)
17. **`Inspiration`** (Text or entity link)
18. **`Backstory`** (Text)
19. **`TriviaFact`** (Text)
20. **`Quote`** (Text, attributedTo: `Person`/`Publication`)
21. **`Publication`** (e.g., magazine, blog)
22. **`Review`**
23. **`Award`**
24. **`Nomination`**
25. **`Film`** (Sub-type of `WorkOfArt`)
26. **`KEXPShow`**
27. **`KEXPSegment`** (e.g., "Song of the Day")
28. **`Listener`**
29. **`ListenerComment`**
30. **`Dedication`**
31. **`TVShow`**
32. **`Instrument`**
33. **`Nationality`**
34. **`Studio`**
35. **`HistoricalEvent`**
36. **`ListenerRequest`**
37. **`LyricSnippet`**

#### Extended Relationships:

- `RECORDED_AT` (Source: `Song`,`WorkOfArt`, Target: `Studio`)
- `LYRICS_WRITTEN_BY` (Song -> Person/Artist)
- `MUSIC_COMPOSED_BY` (Song -> Person/Artist)
- `PRODUCED_BY` (WorkOfArt -> Person/Artist)
- `FEATURED_IN_SOUNDTRACK_OF` (Song -> Film)
- `NAMED_AFTER` (WorkOfArt -> WorkOfArt/Text)
- `RECEIVED_ACCOLADE_FROM` (WorkOfArt/Artist -> Accolade)
- `REVIEW_CONTAINS_QUOTE` (Review -> Quote)
- `DJ_EXPRESSED_OPINION` (Person_DJ -> Text, about `WorkOfArt`/`Artist`)
- `WAS_KEXP_SONG_OF_THE_DAY_ON` (Song -> Date)
- `PARTICIPATED_IN_KEXP_SESSION_ON` (Artist -> Date)
- And many more specific relations derived from the detailed entity list above.

This schema provides a layered approach. Start with the "Core" set and expand as your extraction capabilities and data richness grow.

---

### Document: Categories of KEXP DJ Comments (with Examples)

This document outlines the types of information typically found in KEXP DJ comments, which will inform the features of your knowledge graph.

**1. Artist Information**

- **Description:** Details about the musicians and bands.
- **Sub-categories & Examples:**
  - **Biography & Origins:** Formation details, member names, origin location, key life facts.
    - _"Polly Jean Harvey, MBE, known as PJ Harvey, is an English musician, singer-songwriter, writer, poet, and composer."_ (from play_id: 2614708)
    - _"Mint Field was originally formed in Tijuana by frontwoman Estrella del Sol Sánchez and ex-member Amor Amezcua in 2015."_ (from play_id: 3152436)
  - **Influences:** Artists, genres, or other works that shaped the artist.
    - _"MIKE is a beloved NYC rapper whose earnestly nonchalant post-Earl Sweatshirt/MF Doom influenced stylings are at their best on his latest LP, Showbiz!"_ (from previous examples)
  - **Aliases & Side Projects:** Other names or related musical projects.
    - _"Side project of Ty Segall - with Ex-Cult's Chris Shaw and Fuzz's Charles Moothart."_ (from play_id: 235605)
  - **Roles & Skills:** Specific contributions or talents.
    - _"Primarily known as a vocalist and guitarist, she is also proficient with a wide range of instruments."_ (PJ Harvey, from play_id: 2614708)

**2. Work Information (Song, Album, Release)**

- **Description:** Details about specific musical pieces, albums, or their published versions.
- **Sub-categories & Examples:**
  - **Release Details:** Titles, release dates, record labels, formats.
    - _"From 'All Hail the Queen,' the debut album from Queen Latifiah, released in 1989."_ (from play_id: 2744601)
    - _"Jessie Ware's fifth studio album, That! Feels Good!, just acme out on April 28!"_ (from play_id: 3195585)
  - **Production & Recording:** How the music was made, producers, samples.
    - _"Nia Archives directly sampled Cocoa Tea's 1993 \"18 and Over.\""_ (from previous examples)
    - _"This song was written by Thelonious Monk with lyrics by Sally Swisher."_ (from play_id: 2785763)
  - **Inspiration & Backstory:** Stories behind the creation of the work.
    - _"La Luz's Shana Cleveland... “Faces In The Firelight” is about “watching Will tend to a huge burn pile...""_ (from play_id: 3148381)
  - **Collaborations:** Featured artists or guest musicians.
    - _"Earl King tune, album made with Allen Toussaint and the Meters..."_ (from play_id: 343632)
  - **Sound Description & Genre:** Musical style.
    - _"...the atmospheric shoegaze proggers originally formed in Tijuana..."_ (Mint Field, from play_id: 3152436)
  - **Lyrical Themes & Meaning:** Subject matter of the lyrics.
    - _"Ben Goldwasser says the original refrain...developed into this ambiguous story..."_ (MGMT "Me and Michael", from play_id: 2742372 & 2753205)
  - **Cover Song Information:** If the song is a version of another artist's work.
    - _"Marvin Gaye cover!"_ (from previous examples)

**3. Event & Performance Information**

- **Description:** Details about live shows, broadcasts, or other appearances.
- **Sub-categories & Examples:**
  - **Live Shows & Tours:** Concerts, festivals, tour dates, venues.
    - _"Seattle WA. Playing a crazy FBK/The Mix show on July 4th with ISKRA, VASTATION..."_ (from play_id: 2420875)
    - _"PJ Harvey performed the Iceland Airwaves Music Festival this year."_ (from play_id: 2614708)
  - **Broadcasts & KEXP Sessions:** In-studio performances, special broadcasts.
    - _"Slowdive was in the KEXP studios recently..."_ (from play_id: 97304)
    - _"Ty Segall & White Fence session on KEXP (BROADCAST ONLY) TOMORROW..."_ (from play_id: 235605)

**4. Critical Reception & Impact**

- **Description:** How the music was received, its influence, or accolades.
- **Sub-categories & Examples:**
  - **Awards & Accolades:** Recognition like chart positions or "best of" list mentions.
    - _"Brutalism was selected at number 5 in BBC 6 Music's list '6 Music Recommends Albums Of The Year 2017'..."_ (from play_id: 239074)
  - **Reviews & Quotes:** Excerpts or mentions of reviews.
    - _"This review calls \"Your Silent Face\" \"six minutes of utter bliss\"..."_ (New Order, from play_id: 2687474)

**5. External References & Links**

- **Description:** Pointers to outside resources or connections to other media.
- **Sub-categories & Examples:**
  - **URLs:** Links to websites, articles, videos.
    - _"...KEXP blog? [http://blog.kexp.org/2017/05/04/kexp-exclusive-interview-rachel-goswell-of-slowdive/](http://blog.kexp.org/2017/05/04/kexp-exclusive-interview-rachel-goswell-of-slowdive/)"_ (from play_id: 97304)
    - _"[https://www.facebook.com/events/421952801305751/](https://www.facebook.com/events/421952801305751/)"_ (from play_id: 2420875)
  - **Mentions of Other Works/Media:** References to films, books, etc.
    - _"Named after Robert Altman's film of the same name..."_ (from play_id: 2568136)
    - _"An instrumental version of this song was featured on the soundtrack of \"Pretty in Pink\" (1986)."_ (from play_id: 2568136)

**6. DJ Subjective Commentary**

- **Description:** Personal opinions, dedications, or direct interactions from the DJ.
- **Sub-categories & Examples:**
- **Personal Endorsements:** DJ's personal appreciation.
  - _"Gorgeous late 2023 album released by International Anthem Records from Resavoir..."_ (from play_id: 3335244)
  - **Dedications & Shout-Outs:** Messages to listeners.
    - _"This one's for Jen and Jason, driving down I-95 to Miami to see New Order."_ (from play_id: 2687474)

**7. Trivia & Interesting Facts**

- **Description:** Unique or noteworthy facts about artists, songs, or albums.
- **Sub-categories & Examples:**

  - _"To throw censors off the scent, when \"Relax\" first came out, the band claimed publicly that it was written about \"motivation.\" Later, they confessed it was actually about \"shagging.\""_ (Frankie Goes To Hollywood, from play_id: 2699692)

- **Birth, Death Information, On This Day:**
  - _"Born in 1970 in London, England."_ (from play_id: 2614708)
  - _"Died in 2020 in London, England."_ (from play_id: 2614708)
  - _"On This Day: PJ Harvey was born in 1969 in London, England."_ (from play_id: 2614708)

**8. Listener Interaction**

- **Description:** Content driven by listener input, like requests or messages.
- **Sub-categories & Examples:**
  - _"Hi Troy, loving the show so far! I'm getting free tickets to a show in Brooklyn tonight...Which would you see? Play one of them? Thanks! Jon"_ (from play_id: 3091361)

**9. Station-Specific Information (KEXP)**

- **Description:** Information related to KEXP programming, events, history, or calls to action.
- **Sub-categories & Examples:**
  - _"KEXP's Song Of The Day back in March 2016 -- [http://blog.kexp.org/2016/03/23/song-of-the-day-pj-harvey-the-wheel/](http://blog.kexp.org/2016/03/23/song-of-the-day-pj-harvey-the-wheel/)"_ (from play_id: 2614708)
  - _"We couldn't write reviews of albums like The National's recently released \"Trouble Will Find Me\" without your support, call or donate online today!"_ (from play_id: 2120106)

**10. Show Specific Information**

- **Description:** Information related to KEXP shows, segments, or other show-specific content.
- **Sub-categories & Examples:**
  - _"Today On Swinging Doors: We're playing honky tonk classics from the 50s and 60s, including songs by Hank Williams, Patsy Cline, and more!"_ (from play_id: 2614708)
  - _"Celebrating the works of Brian Eno"_ (from play_id: 2614708)

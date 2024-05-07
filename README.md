# SEEKER
### GOAL: Elucidate lobbyist influence on elections by calculating how their SuperPAC donations are redstributed.
Super PACs allow donors to skirt campaign finance laws which limit how much any organization or individual can give to a single candidate. The laws governing how much Super PACs can take from who and who they can give to are much less stringent, which allows them to functionally launder money for lobbyists.

An added bonus is Super PACs make it difficult to draw a straight line between any organization and a candidate. When they donate to a Super PAC, a lobbyist's money just goes into a big anonymous pool, which is then redistributed as needed. If Comcast donates to a candidate directly, it's easy to demonstrate that that candidate is beholden to Comcast. If Comcast donates to Club For Growth, which then subsequently donates to that candidate, the connection is harder to see.

SEEKER aims to elucidate those connections by calculating how much target industries donate to top tier Super PACs, and how those Super PACs focus their donations.

### PROCESS:
#### 1. Identify industry donors and common Super PACs.
Get Commitee IDs using <a href="https://projects.propublica.org/api-docs/campaign-finance/committees/">ProPublica API</a>. This is mostly done ad hoc, but the fossil fuel donors are pulled broadly from  <a href="https://nofossilfuelmoney.org/company-list/">this fossil fuel donor PAC list</a>.

#### 2. Collect industry donations to Super PACs.
The easiest way to do this is to work backwards from Super PAC receipts (aka Schedule A forms). Get all <a href="https://api.open.fec.gov/developers/#/receipts">Schedule A filings from the OpenFEC API</a>, then filter on industry donor IDs to find relevant donations.

#### 3. Collect Super PAC expenditures.
We are only interested in Independent Expenditures (<a href="https://projects.propublica.org/api-docs/campaign-finance/ies/">aka Schedule E forms, available on the ProPublica API</a>). 

#### 4. Calculate relevant stats.
Filter on state or candidate, or other parameter in question.


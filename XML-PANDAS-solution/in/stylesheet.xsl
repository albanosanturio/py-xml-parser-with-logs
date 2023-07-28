<!--Declares the XSLT stylesheet, version and declares eespace-->
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
  <!--Specifies we will output an xml file-->
  <xsl:output method="xml" indent="yes" />

  <!--Declares the main template (and entry point of transformation)-->
  <xsl:template match="/">
    <!--After matching root node of xml, apply templates to all reading tags-->
    <data>
      <xsl:apply-templates select="//Reading"/>
    </data>
    <!-- </xsl:apply-templaes> -->
  </xsl:template>

  <!--Match reading tags and declare transformation logic for each element-->
  <xsl:template match="Reading">
    <xsl:variable name="Channel" select = "../../../.."/>
    <xsl:variable name="StatusRef" select="@StatusRef"></xsl:variable>
    <!--This is the required output-->
    <Reading>
      <!-- Service Point ID column-->
      <ServicePointID>
        <xsl:choose>
          <xsl:when test="$Channel/ChannelID/@ServicePointChannelID">
            <xsl:value-of select="substring-before($Channel/ChannelID/@ServicePointChannelID, ':')"/>
          </xsl:when>
          <xsl:otherwise>
            <xsl:value-of select="substring-before($Channel/ChannelID/@IntervalChannelID, ':')"/>
          </xsl:otherwise>
        </xsl:choose>
      </ServicePointID>

      <!-- Meter ID column-->
      <MeterID>
        <xsl:choose>
          <xsl:when test="$Channel/ChannelID/@ServicePointChannelID">
            <xsl:value-of select="substring-before($Channel/ChannelID/@ServicePointChannelID, ':')"/>
          </xsl:when>
          <xsl:otherwise>
            <xsl:value-of select="substring-before($Channel/ChannelID/@IntervalChannelID, ':')"/>
          </xsl:otherwise>
        </xsl:choose>
      </MeterID>

      <!-- Channel ID column, not part of the schema but used for calculation of energy direction-->
      <ChannelID>
        <xsl:choose>
          <xsl:when test="$Channel/ChannelID/@ServicePointChannelID">
            <xsl:value-of select="substring-after($Channel/ChannelID/@ServicePointChannelID, ':')"/>
          </xsl:when>
          <xsl:otherwise>
            <xsl:value-of select="substring-after($Channel/ChannelID/@IntervalChannelID, ':')"/>
          </xsl:otherwise>
        </xsl:choose>
      </ChannelID>

      <Timestamp>
        <xsl:value-of select="$Channel/ContiguousIntervalSets/ContiguousIntervalSet/TimePeriod/@StartTime"/>
      </Timestamp>

      <IntervalValue>
        <xsl:value-of select="$Channel/@IntervalLength"/>
      </IntervalValue>

      <Usage>
        <!--Select value of current context node (in this case, current reading)-->
        <xsl:value-of select="./@Value"/>
      </Usage>

      <EnergyType>
        <xsl:value-of select="$Channel/@MarketType"/>
      </EnergyType>

      <EnergyUnits>
        <xsl:text>kWh</xsl:text>
      </EnergyUnits>

      <StatusRef>
        <xsl:value-of select="./@StatusRef"/>
      </StatusRef>

      <!-- This is not a column of the output file per-se but when using a pandas dataframe it can be generated more easily-->
      <IsEstimateOrOutageCodes>
        <xsl:for-each select="//MeterReadingDocument/ReadingStatusRefTable/ReadingStatusRef[@Ref = $StatusRef]/UnencodedStatus/StatusCodes/Code">
            <xsl:value-of select="."/>
            <xsl:if test="position() != last()">
                <xsl:text>,</xsl:text>
            </xsl:if>
        </xsl:for-each>
      </IsEstimateOrOutageCodes>

      <IsDeleted>
        <xsl:text>false</xsl:text>
      </IsDeleted>

      <UpdatedDatetime>
        <xsl:value-of select="//MeterReadingDocument/Header/Creation_Datetime/@Datetime"/> 
      </UpdatedDatetime>

      <!-- TODO: Implement logic for IsEstimate and IsOutage columns-->
      <IsEstimate>
        <!-- Must be based in StatusRef attribute and the codes this contains in metadata -->
        <xsl:text>true</xsl:text>
      </IsEstimate>

      <IsOutage>
        <xsl:text>true</xsl:text>
      </IsOutage>
    </Reading>
  </xsl:template>
</xsl:stylesheet>
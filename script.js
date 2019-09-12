const mssql = require("mssql");
const csv = require("convert-csv-to-json");

const cup = "WorldCups.csv";
const player = "WorldCupPlayers.csv";
const match = "WorldCupMatches.csv";

const config = {
  user: "",
  password: "",
  server: "",
  database: ""
};

const continente = {
  Uruguay: "América do Sul",
  Italy: "Europa",
  France: "Europa",
  Brazil: "América do Sul",
  Switzerland: "Europa",
  Sweden: "Europa",
  Chile: "América do Sul",
  England: "Europa",
  Mexico: "América Central",
  Argentina: "América do Sul",
  Spain: "Europa",
  Italy: "Europa",
  USA: "América do Norte",
  "Korea/Japan": "Ásia",
  Germany: "Europa",
  "South Africa": "África"
};

const meses = {
  May: "Maio",
  Jun: "Junho",
  Jul: "Julho"
};

(async function initDatabase() {
  const sql = await mssql.connect(config);

  const cups = csv.fieldDelimiter(",").getJsonFromCsv(cup);
  const matches = csv.fieldDelimiter(",").getJsonFromCsv(match);
  const players = csv.fieldDelimiter(",").getJsonFromCsv(player);

  for (const data of matches) {
    try {
      const code = "�";

      if (data.Stadium.includes(code)) continue;

      const [cup] = cups.filter(({ Year }) => Year === data.Year);

      const local = {
        Pais: cup.Country,
        Nome_Estadio: data.Stadium.trim(),
        Cidade: data.City.split('"')[1].trim(),
        Continente: continente[cup.Country]
      };

      const Id_Local = await insertLocal(sql, local);

      const datetime = data.Datetime.split(" ");

      const tempo = {
        Dia: datetime[0].slice(1),
        Mes: meses[datetime[1].slice(0, 3)],
        Ano: datetime[2]
      };

      const Id_Tempo = await insertTempo(sql, tempo);

      const partida = {
        Id_Match: data.MatchID,
        Desc_Resultado_Especial:
          data.Winconditions.length > 4 ? data.Winconditions : null,
        Id_Round: data.RoundID
      };

      const Id_Partida = await insertPartida(sql, partida);

      const [timeCasa] = players.filter(
        player =>
          player.MatchID === data.MatchID &&
          player.TeamInitials === data.HomeTeamInitials
      );

      const [timeVisitante] = players.filter(
        player =>
          player.MatchID === data.MatchID &&
          player.TeamInitials === data.AwayTeamInitials
      );

      let time_casa, Id_Time_Casa, time_visitante, Id_Time_Visitante;

      if (timeCasa) {
        time_casa = {
          Nome_Time: data.HomeTeamName,
          Nome_Tecnico: timeCasa.CoachName || null
        };

        Id_Time_Casa = await insertTime(sql, time_casa);
      }

      if (timeVisitante) {
        time_visitante = {
          Nome_Time: data.AwayTeamName,
          Nome_Tecnico: timeVisitante.CoachName || null
        };

        Id_Time_Visitante = await insertTime(sql, time_visitante);
      }

      const fato = {
        Id_Local,
        Id_Partida,
        Id_Time_Casa,
        Id_Time_Visitante,
        Id_Tempo,
        quant_gols_time_casa: parseInt(data.HomeTeamGoals),
        quant_gols_time_visitante: parseInt(data.AwayTeamGoals),
        quant_publico_total: parseInt(data.Attendance),
        quant_gols_primeiro_tempo_time_casa: parseInt(
          data["Half-timeHomeGoals"]
        ),
        quant_gols_segundo_tempo_time_visitante: parseInt(
          data["Half-timeAwayGoals"]
        )
      };

      await insertFato(sql, fato);
      console.log("\nINSERINDO PARTIDA: ", data.MatchID);
    } catch (error) {
      continue;
    }
  }

  console.log("DONE");
  process.exit(0);
})();

async function selectLocal(sql, stadium) {
  const {
    recordset
  } = await sql.query`select * from DIM_LOCAL where Nome_Estadio = ${stadium}`;

  return recordset[0] ? recordset[0].Id_Local : null;
}

async function insertLocal(sql, local) {
  const exists = await selectLocal(sql, local.Nome_Estadio);

  if (exists) return exists; // não insere caso registro já exista, apenas retorna id

  await sql.query`insert into DIM_LOCAL (Nome_Estadio, Cidade, Pais, Continente) values (
    ${local.Nome_Estadio},
    ${local.Cidade},
    ${local.Pais},
    ${local.Continente})`;

  return selectLocal(sql, local.Nome_Estadio);
}

async function selectPartida(sql, matchId) {
  const {
    recordset
  } = await sql.query`select * from DIM_PARTIDA where Id_Match = ${matchId}`;

  return recordset[0] ? recordset[0].Id_Partida : null;
}

async function insertPartida(sql, partida) {
  const exists = await selectPartida(sql, partida.Id_Match);

  if (exists) return exists;

  await sql.query`insert into DIM_PARTIDA (Id_Match, Desc_Resultado_Especial, Id_Round) values (
    ${partida.Id_Match},
    ${partida.Desc_Resultado_Especial},
    ${partida.Id_Round}
  )`;

  return selectPartida(sql, partida.Id_Match);
}

async function selectTime(sql, nomeTime) {
  const {
    recordset
  } = await sql.query`select * from DIM_TIME where Nome_Time = ${nomeTime}`;

  return recordset[0] ? recordset[0].Id_Time : null;
}

async function insertTime(sql, time) {
  const exists = await selectTime(sql, time.Nome_Time);

  if (exists) return exists; // não insere caso registro já exista, apenas retorna id

  await sql.query`insert into DIM_TIME (Nome_Time, Nome_Tecnico) values (
    ${time.Nome_Time},
    ${time.Nome_Tecnico}
  )`;

  return selectTime(sql, time.Nome_Time);
}

async function selectTempo(sql, tempo) {
  const {
    recordset
  } = await sql.query`select * from DIM_TEMPO where Dia = ${tempo.Dia} AND Mes = ${tempo.Mes} AND ANO = ${tempo.Ano}`;

  return recordset[0] ? recordset[0].Id_Tempo : null;
}

async function insertTempo(sql, tempo) {
  const exists = await selectTempo(sql, tempo);

  if (exists) return exists; // não insere caso registro já exista, apenas retorna id

  await sql.query`insert into DIM_TEMPO (Dia, Mes, Ano) values (
    ${tempo.Dia},
    ${tempo.Mes},
    ${tempo.Ano}
  )`;

  return selectTempo(sql, tempo);
}

async function selectFato(sql, partidaId) {
  const {
    recordset
  } = await sql.query`select * from FATO where Id_Partida = ${partidaId}`;

  return recordset[0] ? recordset[0].Id_Fato : null;
}

async function insertFato(sql, fato) {
  const exists = await selectFato(sql, fato.Id_Partida);

  if (exists) return exists; // não insere caso registro já exista, apenas retorna id

  await sql.query`insert into FATO (
    Id_Partida, 
    Id_Local, 
    Id_Time_Casa, 
    Id_Time_Visitante, 
    Id_Tempo, 
    Quant_Gols_Time_Casa, 
    Quant_Gols_Time_Visitante, 
    Quant_Publico_Total, 
    Quant_Gols_Primeiro_Tempo_Time_Casa, 
    Quant_Gols_Primeiro_Tempo_Time_Visitante
    ) values (
    ${fato.Id_Partida},
    ${fato.Id_Local},
    ${fato.Id_Time_Casa},
    ${fato.Id_Time_Visitante},
    ${fato.Id_Tempo},
    ${fato.quant_gols_time_casa},
    ${fato.quant_gols_time_visitante},
    ${fato.quant_publico_total},
    ${fato.quant_gols_primeiro_tempo_time_casa},
    ${fato.quant_gols_segundo_tempo_time_visitante}
  )`;
}

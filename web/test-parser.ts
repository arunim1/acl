// Test script for verifying parser and analysis engine
import { readFileSync } from 'fs';
import { ChessParser } from './lib/chess-parser';
import { AnalysisEngine } from './lib/analysis-engine';

async function testParser() {
  console.log('=== Testing Chess Parser ===\n');

  // Load sample PGN
  const pgnText = readFileSync('./public/head.pgn', 'utf-8');

  console.log('Parsing PGN file...');
  const games = ChessParser.parsePGNText(pgnText, {
    requireClkEval: true,
    minElo: 2000,
  });

  console.log(`\nParsed ${games.length} games`);

  if (games.length > 0) {
    const firstGame = games[0];
    console.log('\nFirst game metadata:');
    console.log('- White:', firstGame.metadata.white);
    console.log('- Black:', firstGame.metadata.black);
    console.log('- White ELO:', firstGame.metadata.whiteElo);
    console.log('- Black ELO:', firstGame.metadata.blackElo);
    console.log('- Time Control:', firstGame.metadata.timeControl);
    console.log('- Moves:', firstGame.moves.length);

    if (firstGame.moves.length > 0) {
      console.log('\nFirst 3 moves:');
      firstGame.moves.slice(0, 3).forEach((move, idx) => {
        console.log(`${idx + 1}. Move ${move.moveNumber}${move.side}:`);
        console.log(`   - Move: ${move.move}`);
        console.log(`   - Eval: ${move.eval}`);
        console.log(`   - Centipawn Loss: ${move.centipawnLoss.toFixed(2)}`);
        console.log(`   - Time Spent: ${move.timeSpent.toFixed(2)}s`);
        console.log(`   - Time Left: ${move.timeLeft}s`);
      });
    }
  }

  console.log('\n=== Testing Analysis Engine ===\n');

  const analysis = AnalysisEngine.analyze(games);

  console.log('Analysis Results:');
  console.log('- Total Games:', analysis.totalGames);
  console.log('- Total Moves:', analysis.totalMoves);
  console.log('- Avg Centipawn Loss:', analysis.avgCentipawnLoss.toFixed(4));
  console.log('- Avg Time Spent:', analysis.avgTimeSpent.toFixed(2), 's');
  console.log('- Time vs Loss data points:', analysis.timeVsLossData.length);
  console.log('- Move distribution data points:', analysis.moveDistribution.length);

  if (analysis.eloImpact) {
    console.log('- ELO impact data points:', analysis.eloImpact.length);
  }

  if (analysis.timeVsLossData.length > 0) {
    console.log('\nFirst 5 time vs loss data points:');
    analysis.timeVsLossData.slice(0, 5).forEach((point) => {
      console.log(
        `  Time: ${point.timeSpent}s -> Avg Loss: ${point.avgLoss.toFixed(2)} ` +
          `(±${point.stdDev.toFixed(2)}, n=${point.count})`
      );
    });
  }

  console.log('\n=== Testing Filters ===\n');

  // Test filtering by player color
  const whiteOnlyAnalysis = AnalysisEngine.analyze(games, {
    playerColor: 'white',
  });

  console.log('White moves only:');
  console.log('- Total Moves:', whiteOnlyAnalysis.totalMoves);
  console.log('- Avg Centipawn Loss:', whiteOnlyAnalysis.avgCentipawnLoss.toFixed(4));

  // Test filtering by move phase
  const openingAnalysis = AnalysisEngine.analyze(games, {
    movePhase: 'opening',
  });

  console.log('\nOpening phase only (moves 1-15):');
  console.log('- Total Moves:', openingAnalysis.totalMoves);
  console.log('- Avg Centipawn Loss:', openingAnalysis.avgCentipawnLoss.toFixed(4));

  console.log('\n=== Testing CSV Export ===\n');

  const csv = AnalysisEngine.exportToCSV(games.slice(0, 2)); // Export first 2 games
  const lines = csv.split('\n');

  console.log('CSV Export:');
  console.log('- Total lines:', lines.length);
  console.log('- Headers:', lines[0]);
  console.log('- First data line:', lines[1]);

  console.log('\n=== All Tests Completed Successfully! ===\n');
}

testParser().catch((error) => {
  console.error('Test failed:', error);
  process.exit(1);
});

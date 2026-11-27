import 'package:flutter/material.dart';
void main() => runApp(const TalimApp());

class TalimApp extends StatelessWidget {
  const TalimApp({super.key});
  @override
  Widget build(BuildContext context) => MaterialApp(
    title: "TA'LIM NAZORATI",
    debugShowCheckedModeBanner: false,
    theme: ThemeData(colorScheme: ColorScheme.fromSeed(seedColor: const Color(0xFF1769AA)),useMaterial3: true,scaffoldBackgroundColor: const Color(0xFFF4F7FB)),
    home: const HomePage(),
  );
}

class HomePage extends StatelessWidget {
  const HomePage({super.key});
  static const sections = <(String,IconData)>[
    ("O‘quvchilar",Icons.people_alt_outlined),("Guruhlar",Icons.groups_outlined),
    ("Davomat",Icons.fact_check_outlined),("Vazifalar",Icons.assignment_outlined),
    ("Testlar",Icons.quiz_outlined),("OMR tekshirish",Icons.document_scanner_outlined),
    ("Rasch tahlili",Icons.analytics_outlined),("To‘lovlar",Icons.payments_outlined),
    ("Hisobotlar",Icons.bar_chart_outlined),("Sozlamalar",Icons.settings_outlined),
  ];
  @override
  Widget build(BuildContext context) => Scaffold(
    appBar: AppBar(title: const Text("TA’LIM NAZORATI",style: TextStyle(fontWeight: FontWeight.w800)),backgroundColor: Colors.white),
    body: ListView(padding: const EdgeInsets.all(16),children: [
      const Text("Bosh sahifa",style: TextStyle(fontSize: 24,fontWeight: FontWeight.bold)),
      const SizedBox(height: 14),
      const Wrap(spacing: 10,runSpacing: 10,children: [
        StatCard("Jami o‘quvchilar",Icons.people),StatCard("Jami testlar",Icons.quiz),
        StatCard("O‘rtacha natija",Icons.trending_up),StatCard("Bugungi davomat",Icons.event_available),
      ]),
      const SizedBox(height: 22),
      const Text("Boshqaruv bo‘limlari",style: TextStyle(fontSize: 18,fontWeight: FontWeight.bold)),
      const SizedBox(height: 8),
      ...sections.map((s) => Card(child: ListTile(leading: Icon(s.$2,color: const Color(0xFF1769AA)),title: Text(s.$1),trailing: const Icon(Icons.chevron_right),onTap: () => Navigator.push(context,MaterialPageRoute(builder: (_) => SectionPage(title:s.$1)))))),
    ]),
    bottomNavigationBar: NavigationBar(destinations: const [
      NavigationDestination(icon: Icon(Icons.home_outlined),label: "Bosh sahifa"),
      NavigationDestination(icon: Icon(Icons.person_outline),label: "Profil"),
      NavigationDestination(icon: Icon(Icons.leaderboard_outlined),label: "Reyting"),
      NavigationDestination(icon: Icon(Icons.notifications_none),label: "Xabarlar"),
      NavigationDestination(icon: Icon(Icons.assessment_outlined),label: "Hisobot"),
    ]),
  );
}

class StatCard extends StatelessWidget {
  final String title; final IconData icon;
  const StatCard(this.title,this.icon,{super.key});
  @override
  Widget build(BuildContext context) => SizedBox(width: 164,child: Card(child: Padding(padding: const EdgeInsets.all(14),child: Column(crossAxisAlignment: CrossAxisAlignment.start,children: [Icon(icon,color: const Color(0xFF1769AA)),const SizedBox(height: 12),const Text("—",style: TextStyle(fontSize: 24,fontWeight: FontWeight.bold)),Text(title,style: const TextStyle(fontSize: 12))]))));
}

class SectionPage extends StatelessWidget {
  final String title;
  const SectionPage({required this.title,super.key});
  @override
  Widget build(BuildContext context) => Scaffold(appBar: AppBar(title: Text(title)),body: Center(child: Text("$title moduli",style: const TextStyle(fontSize: 22,fontWeight: FontWeight.bold))));
}
